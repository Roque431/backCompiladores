package main

import (
	"bufio"
	"crypto/tls"
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
// CONEXIÓN OPTIMIZADA PARA ASTRA DB
// ================================

type CassandraConnection struct {
	session *gocql.Session
	cluster *gocql.ClusterConfig
}

func NewCassandraConnection(hosts []string) (*CassandraConnection, error) {
	cluster := gocql.NewCluster(hosts...)
	
	// Configuración específica para Astra DB Cloud
	cluster.Consistency = gocql.LocalQuorum
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 30 * time.Second
	cluster.Port = 9042
	
	// Configuraciones críticas para Astra
	cluster.DisableInitialHostLookup = true
	cluster.IgnorePeerAddr = true
	cluster.NumConns = 1
	cluster.ProtoVersion = 4
	cluster.Compressor = &gocql.SnappyCompressor{}
	
	// Configuración de reconexión
	cluster.ReconnectInterval = 10 * time.Second
	cluster.MaxPreparedStmts = 1000
	cluster.MaxRoutingKeyInfo = 1000
	
	// Configurar keyspace DESPUÉS de la conexión inicial
	keyspace := os.Getenv("CASSANDRA_KEYSPACE")
	if keyspace != "" {
		fmt.Printf("🗂️  Keyspace objetivo: %s\n", keyspace)
	}
	
	// Para Astra DB - usar Client ID y Client Secret
	if username := os.Getenv("CASSANDRA_USERNAME"); username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: os.Getenv("CASSANDRA_PASSWORD"),
		}
		fmt.Printf("🔑 Configurando autenticación: %s\n", username)
	}
	
	// SSL obligatorio para Astra DB
	if os.Getenv("CASSANDRA_SSL") == "true" {
		cluster.SslOpts = &gocql.SslOptions{
			EnableHostVerification: false,
			Config: &tls.Config{
				InsecureSkipVerify: true,
				ServerName:        hosts[0],
				MinVersion:        tls.VersionTLS12,
				MaxVersion:        tls.VersionTLS13,
				CipherSuites: []uint16{
					tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
					tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
					tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
					tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				},
			},
		}
		fmt.Println("🔒 SSL habilitado con configuración Astra")
	}
	
	fmt.Printf("🔗 Conectando a: %s:%d\n", hosts[0], cluster.Port)
	fmt.Printf("⏰ Timeout: %v\n", cluster.ConnectTimeout)
	fmt.Printf("🔌 Protocolo: v%d\n", cluster.ProtoVersion)
	
	// Intentar conexión sin keyspace primero
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("error conectando a Astra DB: %v", err)
	}
	
	fmt.Println("✅ Conexión inicial establecida")
	
	// Ahora configurar el keyspace si es necesario
	if keyspace != "" {
		fmt.Printf("🗂️  Configurando keyspace: %s\n", keyspace)
		err = session.Query(fmt.Sprintf("USE %s", keyspace)).Exec()
		if err != nil {
			// Si el keyspace no existe, intentar crearlo
			fmt.Printf("⚠️  Keyspace '%s' no existe, intentando crear...\n", keyspace)
			createKeyspaceQuery := fmt.Sprintf(`
				CREATE KEYSPACE IF NOT EXISTS %s 
				WITH replication = {
					'class': 'SimpleStrategy', 
					'replication_factor': 1
				}`, keyspace)
			
			err = session.Query(createKeyspaceQuery).Exec()
			if err != nil {
				fmt.Printf("❌ Error creando keyspace: %v\n", err)
			} else {
				fmt.Printf("✅ Keyspace '%s' creado exitosamente\n", keyspace)
				// Intentar usar el keyspace recién creado
				err = session.Query(fmt.Sprintf("USE %s", keyspace)).Exec()
				if err != nil {
					fmt.Printf("⚠️  Error usando keyspace: %v\n", err)
				}
			}
		} else {
			fmt.Printf("✅ Usando keyspace: %s\n", keyspace)
		}
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
	fmt.Printf("🔍 Ejecutando: %s\n", query)
	
	// Limpiar query
	query = strings.TrimSpace(query)
	if strings.HasSuffix(query, ";") {
		query = strings.TrimSuffix(query, ";")
	}
	
	// Manejo especial para comando USE
	if strings.HasPrefix(strings.ToUpper(query), "USE") {
		parts := strings.Fields(query)
		if len(parts) >= 2 {
			keyspace := parts[1]
			err := c.session.Query(fmt.Sprintf("USE %s", keyspace)).Exec()
			if err != nil {
				return nil, fmt.Errorf("no se puede usar el keyspace '%s': %v", keyspace, err)
			}
			return []map[string]interface{}{{"message": fmt.Sprintf("Usando keyspace '%s'", keyspace)}}, nil
		}
		return nil, fmt.Errorf("sintaxis de USE inválida")
	}
	
	// Para comandos que no devuelven resultados
	upperQuery := strings.ToUpper(query)
	if strings.HasPrefix(upperQuery, "CREATE") ||
		strings.HasPrefix(upperQuery, "DROP") ||
		strings.HasPrefix(upperQuery, "INSERT") ||
		strings.HasPrefix(upperQuery, "UPDATE") ||
		strings.HasPrefix(upperQuery, "DELETE") {
		
		err := c.session.Query(query).Exec()
		if err != nil {
			return nil, fmt.Errorf("error ejecutando comando: %v", err)
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
		return nil, fmt.Errorf("error en consulta: %v", err)
	}
	
	return results, nil
}

// [El resto del código permanece igual: TokenType, Lexer, Parser, etc.]

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
	if os.Getenv("PORT") != "" {
		// En producción, permitir todos los orígenes
		config.AllowAllOrigins = true
	} else {
		// En desarrollo, solo localhost
		config.AllowOrigins = []string{"http://localhost:3000", "http://localhost:5173"}
	}
	config.AllowMethods = []string{"GET", "POST", "OPTIONS"}
	config.AllowHeaders = []string{"*"}
	r.Use(cors.New(config))

	// Ruta raíz
	r.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "Cassandra Command Analyzer + Executor API",
			"version": "1.0.0",
			"endpoints": gin.H{
				"health":  "/health",
				"analyze": "/analyze",
			},
		})
	})

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
// FUNCIONES AUXILIARES
// ================================

func connectToCassandraAsync() {
	if astraHost := os.Getenv("CASSANDRA_HOST"); astraHost != "" {
		fmt.Printf("📡 Conectando a Astra DB: %s\n", astraHost)
		fmt.Printf("🔑 Username: %s\n", os.Getenv("CASSANDRA_USERNAME"))
		fmt.Printf("🗂️  Keyspace: %s\n", os.Getenv("CASSANDRA_KEYSPACE"))
		
		hosts := []string{astraHost}
		var err error
		cassandraConn, err = NewCassandraConnection(hosts)
		if err != nil {
			fmt.Printf("❌ Error conectando a Cassandra: %v\n", err)
			fmt.Println("El analizador funcionará sin ejecución")
			cassandraConn = nil
		} else {
			fmt.Println("🎉 ¡CONECTADO EXITOSAMENTE A ASTRA DB!")
		}
	} else {
		fmt.Println("ℹ️  No hay configuración de Cassandra")
	}
	
	fmt.Printf("🎯 Cassandra disponible: %v\n", cassandraConn != nil)
}

func runInteractiveMode() {
	fmt.Println("🔍 Analizador + Executor Cassandra DB")
	fmt.Println("Comandos: CQL, nodetool, cqlsh")
	fmt.Println("Escribe 'exit' para salir")
	fmt.Println("Para modo servidor: go run cassandra.go server\n")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "exit" {
			break
		}
		if input == "" {
			continue
		}

		response := analyzeAndExecuteCommand(input)

		fmt.Printf("✅ Análisis: %v\n", response.Success)
		if response.Command != nil {
			fmt.Printf("Tipo: %s\n", response.Command.Type)
			if response.Command.CQLQuery != "" {
				fmt.Printf("CQL: %s\n", response.Command.CQLQuery)
			}
		}

		if len(response.Errors) > 0 {
			fmt.Println("❌ Errores:")
			for _, err := range response.Errors {
				fmt.Println("  -", err)
			}
		}

		if len(response.Warnings) > 0 {
			fmt.Println("⚠️  Advertencias:")
			for _, warning := range response.Warnings {
				fmt.Println("  -", warning)
			}
		}

		if response.Executed {
			fmt.Printf("🎯 Ejecutado en Cassandra: %d resultados\n", len(response.ExecutionResult))
		}

		fmt.Println()
	}
}

// ================================
// MAIN FUNCTION CON CONEXIÓN ASÍNCRONA
// ================================

func main() {
	// Auto-detectar modo servidor PRIMERO
	if os.Getenv("PORT") != "" || (len(os.Args) > 1 && os.Args[1] == "server") {
		// MODO SERVIDOR - Iniciar servidor PRIMERO
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080"
		}
		
		fmt.Println("🌐 Modo: PRODUCCIÓN (Render)")
		fmt.Println("🚀 Iniciando servidor HTTP...")
		fmt.Printf("📡 Puerto: %s\n", port)
		
		// Iniciar conexión a Cassandra en goroutine (segundo plano)
		go func() {
			time.Sleep(2 * time.Second) // Esperar un poco para que el servidor inicie
			connectToCassandraAsync()
		}()
		
		// Iniciar servidor HTTP inmediatamente
		r := setupAPI()
		r.Run(":" + port)
		
	} else {
		// MODO DESARROLLO LOCAL
		fmt.Println("💻 Modo: DESARROLLO LOCAL")
		
		// En desarrollo local, conectar normalmente
		hosts := []string{"127.0.0.1"}
		fmt.Println("🐳 Intentando conectar a Cassandra local (Docker)")
		
		var err error
		cassandraConn, err = NewCassandraConnection(hosts)
		if err != nil {
			fmt.Printf("⚠️  No se pudo conectar a Cassandra: %v\n", err)
			fmt.Println("El analizador funcionará sin ejecución")
		} else {
			fmt.Println("✅ Conectado a Cassandra")
		}
		
		// Modo consola interactiva
		runInteractiveMode()
	}

	if cassandraConn != nil {
		cassandraConn.Close()
	}
}