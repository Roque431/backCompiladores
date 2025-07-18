package main

import (
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/gocql/gocql"
)

// ================================
// CONEXIÓN A CASSANDRA ACTUALIZADA
// ================================

type CassandraConnection struct {
	session *gocql.Session
	cluster *gocql.ClusterConfig
}

func NewCassandraConnection(hosts []string) (*CassandraConnection, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 30 * time.Second
	cluster.Port = 9042
	
	// Configurar keyspace si está especificado
	if keyspace := os.Getenv("CASSANDRA_KEYSPACE"); keyspace != "" {
		cluster.Keyspace = keyspace
	}
	
	// Para conexión con autenticación (si está configurada)
	if username := os.Getenv("CASSANDRA_USERNAME"); username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: os.Getenv("CASSANDRA_PASSWORD"),
		}
	}
	
	// SSL si está configurado
	if os.Getenv("CASSANDRA_SSL") == "true" {
		cluster.SslOpts = &gocql.SslOptions{
			EnableHostVerification: false,
		}
	}
	
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("error conectando a Cassandra: %v", err)
	}
	
	return &CassandraConnection{
		session: session,
		cluster: cluster,
	}, nil
}

func (c *CassandraConnection) Close() {
	if c.session != nil {
		c.session.Close()
	}
}

func (c *CassandraConnection) ExecuteQuery(query string) ([]map[string]interface{}, error) {
	fmt.Printf("Ejecutando: %s\n", query)
	
	// Manejo especial para comando USE
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "USE") {
		// Extraer el nombre del keyspace
		parts := strings.Fields(strings.TrimSpace(query))
		if len(parts) >= 2 {
			keyspace := strings.TrimSuffix(parts[1], ";")
			
			// Cerrar sesión actual
			if c.session != nil {
				c.session.Close()
			}
			
			// Crear nueva sesión con el keyspace
			c.cluster.Keyspace = keyspace
			session, err := c.cluster.CreateSession()
			if err != nil {
				return nil, fmt.Errorf("no se puede usar el keyspace '%s': %v", keyspace, err)
			}
			
			c.session = session
			return []map[string]interface{}{{"message": fmt.Sprintf("Usando keyspace '%s'", keyspace)}}, nil
		}
		return nil, fmt.Errorf("sintaxis de USE inválida")
	}
	
	// Para comandos que no devuelven resultados (CREATE, DROP, INSERT, etc.)
	if strings.HasPrefix(strings.ToUpper(query), "CREATE") ||
		strings.HasPrefix(strings.ToUpper(query), "DROP") ||
		strings.HasPrefix(strings.ToUpper(query), "INSERT") ||
		strings.HasPrefix(strings.ToUpper(query), "UPDATE") ||
		strings.HasPrefix(strings.ToUpper(query), "DELETE") {
		
		err := c.session.Query(query).Exec()
		if err != nil {
			return nil, err
		}
		return []map[string]interface{}{{"message": "Comando ejecutado exitosamente"}}, nil
	}
	
	// Para comandos SELECT y DESCRIBE
	iter := c.session.Query(query).Iter()
	defer iter.Close()
	
	var results []map[string]interface{}
	
	for {
		row := make(map[string]interface{})
		if !iter.MapScan(row) {
			break
		}
		results = append(results, row)
	}
	
	if err := iter.Close(); err != nil {
		return nil, err
	}
	
	return results, nil
}

// ================================
// TOKENS SIMPLIFICADOS
// ================================

type TokenType int

const (
	NODETOOL TokenType = iota
	CQLSH
	FLAG
	HOST
	PORT
	KEYSPACE
	PARAMETER
	STRING
	NUMBER
	PATH
	EOF
	INVALID
	CQL_COMMAND
)

type Token struct {
	Type   TokenType `json:"type"`
	Value  string    `json:"value"`
	Line   int       `json:"line"`
	Column int       `json:"column"`
}

// ================================
// ANALIZADOR LÉXICO SIMPLIFICADO
// ================================

type Lexer struct {
	input    string
	position int
	line     int
	column   int
}

func NewLexer(input string) *Lexer {
	return &Lexer{input: input, line: 1, column: 1}
}

func (l *Lexer) Tokenize() []Token {
	var tokens []Token
	
	// Detectar tipo de comando al inicio
	upperInput := strings.ToUpper(strings.TrimSpace(l.input))
	
	if strings.HasPrefix(upperInput, "NODETOOL") {
		return l.tokenizeNodetool()
	} else if strings.HasPrefix(upperInput, "CQLSH") {
		return l.tokenizeCqlsh()
	} else if l.isCQLCommand(upperInput) {
		// Para comandos CQL, crear un solo token con todo el comando
		tokens = append(tokens, Token{
			Type:   CQL_COMMAND,
			Value:  strings.TrimSpace(l.input),
			Line:   1,
			Column: 1,
		})
		tokens = append(tokens, Token{Type: EOF})
		return tokens
	}
	
	// Fallback: tokenizar normalmente
	return l.tokenizeDefault()
}

func (l *Lexer) isCQLCommand(input string) bool {
	cqlCommands := []string{"CREATE", "DROP", "USE", "SELECT", "INSERT", "UPDATE", "DELETE", "DESCRIBE", "SHOW"}
	for _, cmd := range cqlCommands {
		if strings.HasPrefix(input, cmd) {
			return true
		}
	}
	return false
}

func (l *Lexer) tokenizeNodetool() []Token {
	var tokens []Token
	words := strings.Fields(l.input)
	
	for i, word := range words {
		var tokenType TokenType
		
		if i == 0 && strings.ToLower(word) == "nodetool" {
			tokenType = NODETOOL
		} else if strings.HasPrefix(word, "-") {
			tokenType = FLAG
		} else if l.looksLikeIP(word) {
			tokenType = HOST
		} else if l.isNumber(word) {
			tokenType = NUMBER
		} else {
			tokenType = PARAMETER
		}
		
		tokens = append(tokens, Token{
			Type:   tokenType,
			Value:  word,
			Line:   1,
			Column: 1,
		})
	}
	
	tokens = append(tokens, Token{Type: EOF})
	return tokens
}

func (l *Lexer) tokenizeCqlsh() []Token {
	var tokens []Token
	words := strings.Fields(l.input)
	
	for i, word := range words {
		var tokenType TokenType
		
		if i == 0 && strings.ToLower(word) == "cqlsh" {
			tokenType = CQLSH
		} else if strings.HasPrefix(word, "-") {
			tokenType = FLAG
		} else if l.looksLikeIP(word) {
			tokenType = HOST
		} else if l.isNumber(word) {
			tokenType = NUMBER
		} else {
			tokenType = PARAMETER
		}
		
		tokens = append(tokens, Token{
			Type:   tokenType,
			Value:  word,
			Line:   1,
			Column: 1,
		})
	}
	
	tokens = append(tokens, Token{Type: EOF})
	return tokens
}

func (l *Lexer) tokenizeDefault() []Token {
	var tokens []Token
	words := strings.Fields(l.input)
	
	for _, word := range words {
		tokens = append(tokens, Token{
			Type:   PARAMETER,
			Value:  word,
			Line:   1,
			Column: 1,
		})
	}
	
	tokens = append(tokens, Token{Type: EOF})
	return tokens
}

func (l *Lexer) looksLikeIP(word string) bool {
	ipPattern := regexp.MustCompile(`^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$`)
	return ipPattern.MatchString(word)
}

func (l *Lexer) isNumber(word string) bool {
	_, err := strconv.Atoi(word)
	return err == nil
}

// ================================
// ESTRUCTURAS DE COMANDOS
// ================================

type Command struct {
	Type       string            `json:"type"`
	Tool       string            `json:"tool"`
	Subcommand string            `json:"subcommand"`
	Flags      map[string]string `json:"flags"`
	Arguments  []string          `json:"arguments"`
	CQLQuery   string            `json:"cql_query"`
}

// ================================
// ANALIZADOR SINTÁCTICO SIMPLIFICADO
// ================================

type Parser struct {
	tokens []Token
	input  string
}

func NewParser(tokens []Token, originalInput string) *Parser {
	return &Parser{
		tokens: tokens,
		input:  originalInput,
	}
}

func (p *Parser) Parse() (*Command, []string) {
	var errors []string

	if len(p.tokens) == 0 {
		return nil, []string{"No hay tokens"}
	}

	cmd := &Command{
		Flags:     make(map[string]string),
		Arguments: []string{},
	}

	// Primer token determina el tipo de comando
	token := p.tokens[0]
	switch token.Type {
	case NODETOOL:
		cmd.Type = "nodetool"
		cmd.Tool = "nodetool"
		return p.parseNodetoolCommand(cmd)
	case CQLSH:
		cmd.Type = "cqlsh"
		cmd.Tool = "cqlsh"
		return p.parseCqlshCommand(cmd)
	case CQL_COMMAND:
		cmd.Type = "cql"
		cmd.Tool = "cql"
		cmd.CQLQuery = token.Value
		cmd.Subcommand = strings.ToUpper(strings.Fields(token.Value)[0])
		return cmd, errors
	default:
		// Intentar como CQL
		cmd.Type = "cql"
		cmd.Tool = "cql"
		cmd.CQLQuery = p.input
		if len(p.tokens) > 0 {
			cmd.Subcommand = strings.ToUpper(p.tokens[0].Value)
		}
		return cmd, errors
	}
}

func (p *Parser) parseNodetoolCommand(cmd *Command) (*Command, []string) {
	var errors []string
	
	// Parsear tokens de nodetool
	for i := 1; i < len(p.tokens) && p.tokens[i].Type != EOF; i++ {
		token := p.tokens[i]
		
		switch token.Type {
		case FLAG:
			flag := token.Value
			// Buscar el valor del flag
			if i+1 < len(p.tokens) && p.tokens[i+1].Type != FLAG && p.tokens[i+1].Type != EOF {
				cmd.Flags[flag] = p.tokens[i+1].Value
				i++ // Saltar el valor
			} else {
				cmd.Flags[flag] = "true"
			}
		case PARAMETER:
			if cmd.Subcommand == "" {
				cmd.Subcommand = token.Value
			} else {
				cmd.Arguments = append(cmd.Arguments, token.Value)
			}
		default:
			cmd.Arguments = append(cmd.Arguments, token.Value)
		}
	}
	
	return cmd, errors
}

func (p *Parser) parseCqlshCommand(cmd *Command) (*Command, []string) {
	var errors []string
	
	// Parsear tokens de cqlsh
	for i := 1; i < len(p.tokens) && p.tokens[i].Type != EOF; i++ {
		token := p.tokens[i]
		
		switch token.Type {
		case FLAG:
			flag := token.Value
			if i+1 < len(p.tokens) && p.tokens[i+1].Type != FLAG && p.tokens[i+1].Type != EOF {
				cmd.Flags[flag] = p.tokens[i+1].Value
				i++
			} else {
				cmd.Flags[flag] = "true"
			}
		default:
			cmd.Arguments = append(cmd.Arguments, token.Value)
		}
	}
	
	return cmd, errors
}

// ================================
// ANALIZADOR SEMÁNTICO
// ================================

type SemanticAnalyzer struct{}

func NewSemanticAnalyzer() *SemanticAnalyzer {
	return &SemanticAnalyzer{}
}

func (s *SemanticAnalyzer) Analyze(cmd *Command) ([]string, []string) {
	var errors []string
	var warnings []string

	switch cmd.Type {
	case "nodetool":
		e, w := s.validateNodetool(cmd)
		errors = append(errors, e...)
		warnings = append(warnings, w...)
	case "cqlsh":
		e, w := s.validateCqlsh(cmd)
		errors = append(errors, e...)
		warnings = append(warnings, w...)
	case "cql":
		e, w := s.validateCQL(cmd)
		errors = append(errors, e...)
		warnings = append(warnings, w...)
	}

	return errors, warnings
}

func (s *SemanticAnalyzer) validateCQL(cmd *Command) ([]string, []string) {
	var errors []string
	var warnings []string
	
	if cmd.CQLQuery == "" {
		errors = append(errors, "Consulta CQL vacía")
		return errors, warnings
	}
	
	upperQuery := strings.ToUpper(cmd.CQLQuery)
	
	if strings.HasPrefix(upperQuery, "CREATE KEYSPACE") {
		if !strings.Contains(upperQuery, "REPLICATION") {
			warnings = append(warnings, "CREATE KEYSPACE sin especificar replicación explícita")
		}
	}
	
	if strings.HasPrefix(upperQuery, "DROP") {
		warnings = append(warnings, "Comando DROP puede ser destructivo")
	}
	
	return errors, warnings
}

func (s *SemanticAnalyzer) validateNodetool(cmd *Command) ([]string, []string) {
	var errors []string
	var warnings []string

	validSubcommands := []string{
		"status", "info", "ring", "flush", "compact", "repair", "snapshot",
		"cleanup", "move", "removenode", "decommission", "drain", "stop",
	}

	if cmd.Subcommand != "" {
		valid := false
		for _, validSub := range validSubcommands {
			if cmd.Subcommand == validSub {
				valid = true
				break
			}
		}
		if !valid {
			errors = append(errors, "Subcomando de nodetool inválido: "+cmd.Subcommand)
		}
	}

	switch cmd.Subcommand {
	case "repair":
		if len(cmd.Arguments) > 2 {
			errors = append(errors, "repair acepta máximo 2 argumentos")
		}
		warnings = append(warnings, "repair puede ser costoso en clusters grandes")
	case "removenode":
		if len(cmd.Arguments) != 1 {
			errors = append(errors, "removenode requiere Host ID")
		} else {
			uuidPattern := `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`
			if matched, _ := regexp.MatchString(uuidPattern, cmd.Arguments[0]); !matched {
				errors = append(errors, "Host ID debe ser UUID válido")
			}
		}
		warnings = append(warnings, "removenode es irreversible")
	case "decommission":
		warnings = append(warnings, "decommission eliminará el nodo permanentemente")
	}

	return errors, warnings
}

func (s *SemanticAnalyzer) validateCqlsh(cmd *Command) ([]string, []string) {
	var errors []string
	var warnings []string

	if cmd.Flags["-u"] != "" && cmd.Flags["-pw"] == "" {
		warnings = append(warnings, "Usuario especificado sin contraseña")
	}

	if file := cmd.Flags["-f"]; file != "" {
		if !strings.HasSuffix(file, ".cql") {
			warnings = append(warnings, "Archivo no tiene extensión .cql")
		}
	}

	return errors, warnings
}

// ================================
// API CON EJECUCIÓN
// ================================

type AnalysisRequest struct {
	Command string `json:"command" binding:"required"`
}

type AnalysisResponse struct {
	Success        bool                     `json:"success"`
	Tokens         []Token                  `json:"tokens"`
	Command        *Command                 `json:"command"`
	Errors         []string                 `json:"errors"`
	Warnings       []string                 `json:"warnings"`
	Executed       bool                     `json:"executed"`
	ExecutionResult []map[string]interface{} `json:"execution_result"`
	Timestamp      time.Time                `json:"timestamp"`
}

var cassandraConn *CassandraConnection

func setupAPI() *gin.Engine {
	// Configurar Gin para producción
	if os.Getenv("PORT") != "" {
		gin.SetMode(gin.ReleaseMode)
	}
	
	r := gin.Default()

	// CORS más permisivo para producción
	config := cors.DefaultConfig()
	config.AllowAllOrigins = true
	config.AllowMethods = []string{"GET", "POST", "OPTIONS"}
	config.AllowHeaders = []string{"*"}
	r.Use(cors.New(config))

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":    "OK",
			"service":   "Cassandra Command Analyzer + Executor",
			"timestamp": time.Now(),
			"cassandra_connected": cassandraConn != nil,
		})
	})

	r.POST("/analyze", func(c *gin.Context) {
		var req AnalysisRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		response := analyzeAndExecuteCommand(req.Command)
		c.JSON(http.StatusOK, response)
	})

	return r
}

func analyzeAndExecuteCommand(input string) *AnalysisResponse {
	response := &AnalysisResponse{
		Timestamp: time.Now(),
	}

	// 1. Análisis Léxico
	lexer := NewLexer(input)
	tokens := lexer.Tokenize()
	response.Tokens = tokens

	// 2. Análisis Sintáctico
	parser := NewParser(tokens, input)
	cmd, syntaxErrors := parser.Parse()

	if len(syntaxErrors) > 0 {
		response.Success = false
		response.Errors = syntaxErrors
		return response
	}

	response.Command = cmd

	// 3. Análisis Semántico
	semantic := NewSemanticAnalyzer()
	errors, warnings := semantic.Analyze(cmd)

	response.Errors = errors
	response.Warnings = warnings
	response.Success = len(errors) == 0

	// 4. Ejecutar si es válido y es CQL
	if response.Success && cmd.Type == "cql" && cassandraConn != nil {
		result, err := cassandraConn.ExecuteQuery(cmd.CQLQuery)
		if err != nil {
			response.Success = false
			response.Errors = append(response.Errors, "Error ejecutando en Cassandra: "+err.Error())
		} else {
			response.Executed = true
			response.ExecutionResult = result
		}
	}

	return response
}

// ================================
// MAIN FUNCTION CORREGIDA
// ================================

func main() {
	fmt.Println("🚀 Iniciando Cassandra Analyzer API...")
	
	// CONFIGURACIÓN DIRECTA PARA AWS CASSANDRA
	hosts := []string{"44.210.182.201"}
	
	fmt.Printf("📡 Conectando a Cassandra en AWS: %s\n", hosts[0])
	
	// Intentar conexión a Cassandra
	var err error
	cassandraConn, err = NewCassandraConnection(hosts)
	if err != nil {
		fmt.Printf("⚠️  Error conectando a Cassandra: %v\n", err)
		fmt.Println("🔄 La API funcionará sin ejecución de comandos")
		cassandraConn = nil
	} else {
		fmt.Println("✅ Conectado a Cassandra AWS exitosamente!")
	}

	// MODO SERVIDOR (siempre en producción)
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	
	fmt.Printf("🌐 Servidor iniciado en puerto: %s\n", port)
	fmt.Printf("🎯 Cassandra disponible: %v\n", cassandraConn != nil)
	fmt.Println("🔗 API URL: /analyze (POST)")
	fmt.Println("🩺 Health check: /health (GET)")
	
	r := setupAPI()
	
	// Iniciar servidor
	if err := r.Run(":" + port); err != nil {
		fmt.Printf("❌ Error iniciando servidor: %v\n", err)
	}
	
	// Cleanup
	if cassandraConn != nil {
		cassandraConn.Close()
	}
}