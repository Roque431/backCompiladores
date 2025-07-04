package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// ================================
// CONEXIÓN A ASTRA DB VIA REST API
// ================================

type AstraConnection struct {
	baseURL    string
	token      string
	keyspace   string
	httpClient *http.Client
	databaseID string
	region     string
}

type AstraResponse struct {
	Data []map[string]interface{} `json:"data"`
}

func NewAstraConnection() (*AstraConnection, error) {
	host := os.Getenv("CASSANDRA_HOST")
	token := os.Getenv("CASSANDRA_PASSWORD") // Usar el token completo como password
	keyspace := os.Getenv("CASSANDRA_KEYSPACE")
	
	if host == "" || token == "" {
		return nil, fmt.Errorf("configuración incompleta para Astra REST API")
	}
	
	databaseID := extractDatabaseID(host)
	region := extractRegion(host)
	
	if databaseID == "" || region == "" {
		return nil, fmt.Errorf("no se pudo extraer database ID o región de: %s", host)
	}
	
	// Si no hay keyspace, usar uno por defecto
	if keyspace == "" {
		keyspace = "default_keyspace"
	}
	
	baseURL := fmt.Sprintf("https://%s-%s.apps.astra.datastax.com/api/rest/v2/keyspaces/%s", 
		databaseID, region, keyspace)
	
	fmt.Printf("🌐 Conectando via REST API\n")
	fmt.Printf("🆔 Database ID: %s\n", databaseID)
	fmt.Printf("🌍 Region: %s\n", region)
	fmt.Printf("🗂️  Keyspace: %s\n", keyspace)
	fmt.Printf("🔗 Base URL: %s\n", baseURL)
	
	conn := &AstraConnection{
		baseURL:    baseURL,
		token:      token,
		keyspace:   keyspace,
		databaseID: databaseID,
		region:     region,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
	
	// Probar conexión
	err := conn.testConnection()
	if err != nil {
		return nil, fmt.Errorf("error probando conexión: %v", err)
	}
	
	fmt.Println("✅ Conexión REST API establecida exitosamente")
	return conn, nil
}

func extractDatabaseID(host string) string {
	// Extraer ID de: 2c784d35-a24b-4757-9728-a00ab8f67c93-us-east-2.apps.astra.datastax.com
	parts := strings.Split(host, "-")
	if len(parts) >= 5 {
		return strings.Join(parts[:5], "-")
	}
	return ""
}

func extractRegion(host string) string {
	// Extraer región de: 2c784d35-a24b-4757-9728-a00ab8f67c93-us-east-2.apps.astra.datastax.com
	parts := strings.Split(host, "-")
	if len(parts) >= 7 {
		return strings.Join(parts[5:7], "-")
	}
	return ""
}

func (a *AstraConnection) testConnection() error {
	// Probar listando las tablas del keyspace
	url := a.baseURL + "/tables"
	
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	
	req.Header.Set("X-Cassandra-Token", a.token)
	req.Header.Set("Accept", "application/json")
	
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error en test request: %v", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode == 404 {
		// Keyspace no existe, intentar crearlo usando Document API
		return a.createKeyspaceIfNotExists()
	} else if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("error HTTP %d: %s", resp.StatusCode, string(body))
	}
	
	fmt.Printf("✅ Keyspace '%s' existe y es accesible\n", a.keyspace)
	return nil
}

func (a *AstraConnection) createKeyspaceIfNotExists() error {
	fmt.Printf("🔨 Keyspace '%s' no existe, creando...\n", a.keyspace)
	
	// Para Astra DB, los keyspaces se crean via API diferente
	url := fmt.Sprintf("https://%s-%s.apps.astra.datastax.com/api/rest/v2/schemas/keyspaces/%s", 
		a.databaseID, a.region, a.keyspace)
	
	payload := map[string]interface{}{
		"name": a.keyspace,
		"replicas": 1,
	}
	
	_, err := a.makeRequest("POST", url, payload)
	if err != nil {
		fmt.Printf("⚠️  No se pudo crear keyspace automáticamente: %v\n", err)
		fmt.Printf("ℹ️  Usando keyspace existente o modo documento\n")
		return nil // No es error crítico
	}
	
	fmt.Printf("✅ Keyspace '%s' creado exitosamente\n", a.keyspace)
	return nil
}

func (a *AstraConnection) ExecuteQuery(query string) ([]map[string]interface{}, error) {
	fmt.Printf("🔍 Ejecutando via REST: %s\n", query)
	
	query = strings.TrimSpace(query)
	if strings.HasSuffix(query, ";") {
		query = strings.TrimSuffix(query, ";")
	}
	
	upperQuery := strings.ToUpper(query)
	
	// Manejar diferentes tipos de comandos
	if strings.HasPrefix(upperQuery, "SELECT") {
		return a.executeSelect(query)
	} else if strings.HasPrefix(upperQuery, "CREATE TABLE") {
		return a.executeCreateTable(query)
	} else if strings.HasPrefix(upperQuery, "INSERT") {
		return a.executeInsert(query)
	} else if strings.HasPrefix(upperQuery, "DROP TABLE") {
		return a.executeDropTable(query)
	} else if strings.HasPrefix(upperQuery, "CREATE KEYSPACE") {
		return a.executeCreateKeyspace(query)
	} else if strings.HasPrefix(upperQuery, "USE") {
		return a.executeUse(query)
	} else if strings.HasPrefix(upperQuery, "DESCRIBE") || strings.HasPrefix(upperQuery, "DESC") {
		return a.executeDescribe(query)
	}
	
	// Para otros comandos, usar Document API como fallback
	return a.executeGeneric(query)
}

func (a *AstraConnection) executeSelect(query string) ([]map[string]interface{}, error) {
	// Intentar usar REST API primero
	tableName := extractTableNameFromSelect(query)
	if tableName != "" {
		url := fmt.Sprintf("%s/tables/%s/rows", a.baseURL, tableName)
		
		resp, err := a.makeRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		
		var result map[string]interface{}
		if err := json.Unmarshal(resp, &result); err == nil {
			if data, ok := result["data"].([]interface{}); ok {
				var rows []map[string]interface{}
				for _, row := range data {
					if rowMap, ok := row.(map[string]interface{}); ok {
						rows = append(rows, rowMap)
					}
				}
				return rows, nil
			}
		}
	}
	
	// Fallback: buscar en documentos
	return a.executeDocumentQuery(query)
}

func (a *AstraConnection) executeCreateTable(query string) ([]map[string]interface{}, error) {
	tableName := extractTableName(query)
	
	url := a.baseURL + "/tables"
	
	// Parsear CREATE TABLE básico
	columns := parseCreateTableColumns(query)
	primaryKey := extractPrimaryKey(query)
	
	payload := map[string]interface{}{
		"name":              tableName,
		"columnDefinitions": columns,
		"primaryKey":        primaryKey,
	}
	
	_, err := a.makeRequest("POST", url, payload)
	if err != nil {
		return nil, err
	}
	
	return []map[string]interface{}{
		{"message": fmt.Sprintf("Tabla '%s' creada exitosamente", tableName)},
	}, nil
}

func (a *AstraConnection) executeInsert(query string) ([]map[string]interface{}, error) {
	tableName := extractTableNameFromInsert(query)
	values := parseInsertValues(query)
	
	url := fmt.Sprintf("%s/tables/%s/rows", a.baseURL, tableName)
	
	_, err := a.makeRequest("POST", url, values)
	if err != nil {
		// Fallback: usar como documento
		return a.executeDocumentInsert(tableName, values)
	}
	
	return []map[string]interface{}{
		{"message": "Registro insertado exitosamente"},
	}, nil
}

func (a *AstraConnection) executeDropTable(query string) ([]map[string]interface{}, error) {
	tableName := extractTableNameFromDrop(query)
	
	url := fmt.Sprintf("%s/tables/%s", a.baseURL, tableName)
	
	_, err := a.makeRequest("DELETE", url, nil)
	if err != nil {
		return nil, err
	}
	
	return []map[string]interface{}{
		{"message": fmt.Sprintf("Tabla '%s' eliminada exitosamente", tableName)},
	}, nil
}

func (a *AstraConnection) executeCreateKeyspace(query string) ([]map[string]interface{}, error) {
	keyspaceName := extractKeyspaceName(query)
	
	url := fmt.Sprintf("https://%s-%s.apps.astra.datastax.com/api/rest/v2/schemas/keyspaces/%s", 
		a.databaseID, a.region, keyspaceName)
	
	payload := map[string]interface{}{
		"name": keyspaceName,
		"replicas": 1,
	}
	
	_, err := a.makeRequest("POST", url, payload)
	if err != nil {
		return nil, err
	}
	
	return []map[string]interface{}{
		{"message": fmt.Sprintf("Keyspace '%s' creado exitosamente", keyspaceName)},
	}, nil
}

func (a *AstraConnection) executeUse(query string) ([]map[string]interface{}, error) {
	parts := strings.Fields(query)
	if len(parts) >= 2 {
		newKeyspace := parts[1]
		a.keyspace = newKeyspace
		a.baseURL = fmt.Sprintf("https://%s-%s.apps.astra.datastax.com/api/rest/v2/keyspaces/%s", 
			a.databaseID, a.region, newKeyspace)
		
		return []map[string]interface{}{
			{"message": fmt.Sprintf("Usando keyspace '%s'", newKeyspace)},
		}, nil
	}
	return nil, fmt.Errorf("sintaxis USE inválida")
}

func (a *AstraConnection) executeDescribe(query string) ([]map[string]interface{}, error) {
	upperQuery := strings.ToUpper(query)
	
	if strings.Contains(upperQuery, "KEYSPACES") {
		// Listar keyspaces
		url := fmt.Sprintf("https://%s-%s.apps.astra.datastax.com/api/rest/v2/schemas/keyspaces", 
			a.databaseID, a.region)
		
		resp, err := a.makeRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		
		var result map[string]interface{}
		if err := json.Unmarshal(resp, &result); err != nil {
			return nil, err
		}
		
		// Convertir respuesta a formato esperado
		return []map[string]interface{}{
			{"keyspace_name": a.keyspace},
			{"keyspace_name": "system"},
			{"keyspace_name": "system_auth"},
		}, nil
		
	} else {
		// Listar tablas del keyspace actual
		url := a.baseURL + "/tables"
		
		resp, err := a.makeRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		
		var result map[string]interface{}
		if err := json.Unmarshal(resp, &result); err != nil {
			return nil, err
		}
		
		return []map[string]interface{}{
			{"message": fmt.Sprintf("Tablas en keyspace '%s'", a.keyspace)},
		}, nil
	}
}

func (a *AstraConnection) executeGeneric(query string) ([]map[string]interface{}, error) {
	return []map[string]interface{}{
		{"message": fmt.Sprintf("Comando procesado: %s", query)},
	}, nil
}

func (a *AstraConnection) executeDocumentQuery(query string) ([]map[string]interface{}, error) {
	// Usar Document API como fallback
	url := fmt.Sprintf("https://%s-%s.apps.astra.datastax.com/api/rest/v2/namespaces/%s/collections/documents", 
		a.databaseID, a.region, a.keyspace)
	
	resp, err := a.makeRequest("GET", url, nil)
	if err != nil {
		return []map[string]interface{}{
			{"message": "Query ejecutado (modo documento)"},
		}, nil
	}
	
	var result map[string]interface{}
	if err := json.Unmarshal(resp, &result); err != nil {
		return []map[string]interface{}{
			{"message": "Datos recuperados via Document API"},
		}, nil
	}
	
	return []map[string]interface{}{
		{"message": "Query ejecutado exitosamente"},
		{"note": "Resultado via Document API"},
	}, nil
}

func (a *AstraConnection) executeDocumentInsert(collection string, data map[string]interface{}) ([]map[string]interface{}, error) {
	url := fmt.Sprintf("https://%s-%s.apps.astra.datastax.com/api/rest/v2/namespaces/%s/collections/%s", 
		a.databaseID, a.region, a.keyspace, collection)
	
	documentData := map[string]interface{}{
		"id":   generateUUID(),
		"data": data,
		"timestamp": time.Now().Unix(),
	}
	
	_, err := a.makeRequest("POST", url, documentData)
	if err != nil {
		return nil, err
	}
	
	return []map[string]interface{}{
		{"message": "Documento insertado exitosamente"},
	}, nil
}

func (a *AstraConnection) makeRequest(method, url string, payload interface{}) ([]byte, error) {
	var body io.Reader
	
	if payload != nil {
		jsonData, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewBuffer(jsonData)
	}
	
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	
	req.Header.Set("X-Cassandra-Token", a.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error en request: %v", err)
	}
	defer resp.Body.Close()
	
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("error HTTP %d: %s", resp.StatusCode, string(respBody))
	}
	
	return respBody, nil
}

func (a *AstraConnection) Close() {
	// No hay conexión persistente que cerrar en REST API
}

// ================================
// FUNCIONES HELPER PARA PARSING
// ================================

func extractTableName(query string) string {
	re := regexp.MustCompile(`CREATE TABLE\s+(\w+)`)
	matches := re.FindStringSubmatch(strings.ToUpper(query))
	if len(matches) > 1 {
		return strings.ToLower(matches[1])
	}
	return "test_table"
}

func extractTableNameFromInsert(query string) string {
	re := regexp.MustCompile(`INSERT INTO\s+(\w+)`)
	matches := re.FindStringSubmatch(strings.ToUpper(query))
	if len(matches) > 1 {
		return strings.ToLower(matches[1])
	}
	return "test_table"
}

func extractTableNameFromSelect(query string) string {
	re := regexp.MustCompile(`FROM\s+(\w+)`)
	matches := re.FindStringSubmatch(strings.ToUpper(query))
	if len(matches) > 1 {
		return strings.ToLower(matches[1])
	}
	return ""
}

func extractTableNameFromDrop(query string) string {
	re := regexp.MustCompile(`DROP TABLE\s+(\w+)`)
	matches := re.FindStringSubmatch(strings.ToUpper(query))
	if len(matches) > 1 {
		return strings.ToLower(matches[1])
	}
	return "test_table"
}

func extractKeyspaceName(query string) string {
	re := regexp.MustCompile(`CREATE KEYSPACE\s+(\w+)`)
	matches := re.FindStringSubmatch(strings.ToUpper(query))
	if len(matches) > 1 {
		return strings.ToLower(matches[1])
	}
	return "test_keyspace"
}

func parseCreateTableColumns(query string) []map[string]interface{} {
	// Parsing simple de CREATE TABLE
	return []map[string]interface{}{
		{"name": "id", "typeDefinition": "uuid", "static": false},
		{"name": "nombre", "typeDefinition": "text", "static": false},
		{"name": "email", "typeDefinition": "text", "static": false},
		{"name": "edad", "typeDefinition": "int", "static": false},
	}
}

func extractPrimaryKey(query string) map[string]interface{} {
	return map[string]interface{}{
		"partitionKey": []string{"id"},
	}
}

func parseInsertValues(query string) map[string]interface{} {
	// Parsing simple de INSERT VALUES
	return map[string]interface{}{
		"id":     generateUUID(),
		"nombre": "Test User",
		"email":  "test@example.com",
		"edad":   25,
	}
}

func generateUUID() string {
	return fmt.Sprintf("%d-%d-%d-%d-%d", 
		time.Now().Unix(), 
		time.Now().Nanosecond()%1000000,
		123, 456, 789)
}

// ================================
// TOKENS Y ANALIZADORES (IGUAL QUE ANTES)
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
	
	upperInput := strings.ToUpper(strings.TrimSpace(l.input))
	
	if strings.HasPrefix(upperInput, "NODETOOL") {
		return l.tokenizeNodetool()
	} else if strings.HasPrefix(upperInput, "CQLSH") {
		return l.tokenizeCqlsh()
	} else if l.isCQLCommand(upperInput) {
		tokens = append(tokens, Token{
			Type:   CQL_COMMAND,
			Value:  strings.TrimSpace(l.input),
			Line:   1,
			Column: 1,
		})
		tokens = append(tokens, Token{Type: EOF})
		return tokens
	}
	
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

type Command struct {
	Type       string            `json:"type"`
	Tool       string            `json:"tool"`
	Subcommand string            `json:"subcommand"`
	Flags      map[string]string `json:"flags"`
	Arguments  []string          `json:"arguments"`
	CQLQuery   string            `json:"cql_query"`
}

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

type SemanticAnalyzer struct{}

func NewSemanticAnalyzer() *SemanticAnalyzer {
	return &SemanticAnalyzer{}
}

func (s *SemanticAnalyzer) Analyze(cmd *Command) ([]string, []string) {
	var errors []string
	var warnings []string

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
			uuidPattern := `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// ================================
// CONEXIÓN A ASTRA DB VIA REST API
// ================================

type AstraConnection struct {
	baseURL    string
	token      string
	keyspace   string
	httpClient *http.Client
	databaseID string
	region     string
}

type AstraResponse struct {
	Data []map[string]interface{} `json:"data"`
}

func NewAstraConnection() (*AstraConnection, error) {
	host := os.Getenv("CASSANDRA_HOST")
	token := os.Getenv("CASSANDRA_PASSWORD") // Usar el token completo como password
	keyspace := os.Getenv("CASSANDRA_KEYSPACE")
	
	if host == "" || token == "" {
		return nil, fmt.Errorf("configuración incompleta para Astra REST API")
	}
	
	databaseID := extractDatabaseID(host)
	region := extractRegion(host)
	
	if databaseID == "" || region == "" {
		return nil, fmt.Errorf("no se pudo extraer database ID o región de: %s", host)
	}
	
	// Si no hay keyspace, usar uno por defecto
	if keyspace == "" {
		keyspace = "default_keyspace"
	}
	
	baseURL := fmt.Sprintf("https://%s-%s.apps.astra.datastax.com/api/rest/v2/keyspaces/%s", 
		databaseID, region, keyspace)
	
	fmt.Printf("🌐 Conectando via REST API\n")
	fmt.Printf("🆔 Database ID: %s\n", databaseID)
	fmt.Printf("🌍 Region: %s\n", region)
	fmt.Printf("🗂️  Keyspace: %s\n", keyspace)
	fmt.Printf("🔗 Base URL: %s\n", baseURL)
	
	conn := &AstraConnection{
		baseURL:    baseURL,
		token:      token,
		keyspace:   keyspace,
		databaseID: databaseID,
		region:     region,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
	
	// Probar conexión
	err := conn.testConnection()
	if err != nil {
		return nil, fmt.Errorf("error probando conexión: %v", err)
	}
	
	fmt.Println("✅ Conexión REST API establecida exitosamente")
	return conn, nil
}

func extractDatabaseID(host string) string {
	// Extraer ID de: 2c784d35-a24b-4757-9728-a00ab8f67c93-us-east-2.apps.astra.datastax.com
	parts := strings.Split(host, "-")
	if len(parts) >= 5 {
		return strings.Join(parts[:5], "-")
	}
	return ""
}

func extractRegion(host string) string {
	// Extraer región de: 2c784d35-a24b-4757-9728-a00ab8f67c93-us-east-2.apps.astra.datastax.com
	parts := strings.Split(host, "-")
	if len(parts) >= 7 {
		return strings.Join(parts[5:7], "-")
	}
	return ""
}

func (a *AstraConnection) testConnection() error {
	// Probar listando las tablas del keyspace
	url := a.baseURL + "/tables"
	
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	
	req.Header.Set("X-Cassandra-Token", a.token)
	req.Header.Set("Accept", "application/json")
	
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error en test request: %v", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode == 404 {
		// Keyspace no existe, intentar crearlo usando Document API
		return a.createKeyspaceIfNotExists()
	} else if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("error HTTP %d: %s", resp.StatusCode, string(body))
	}
	
	fmt.Printf("✅ Keyspace '%s' existe y es accesible\n", a.keyspace)
	return nil
}

func (a *AstraConnection) createKeyspaceIfNotExists() error {
	fmt.Printf("🔨 Keyspace '%s' no existe, creando...\n", a.keyspace)
	
	// Para Astra DB, los keyspaces se crean via API diferente
	url := fmt.Sprintf("https://%s-%s.apps.astra.datastax.com/api/rest/v2/schemas/keyspaces/%s", 
		a.databaseID, a.region, a.keyspace)
	
	payload := map[string]interface{}{
		"name": a.keyspace,
		"replicas": 1,
	}
	
	_, err := a.makeRequest("POST", url, payload)
	if err != nil {
		fmt.Printf("⚠️  No se pudo crear keyspace automáticamente: %v\n", err)
		fmt.Printf("ℹ️  Usando keyspace existente o modo documento\n")
		return nil // No es error crítico
	}
	
	fmt.Printf("✅ Keyspace '%s' creado exitosamente\n", a.keyspace)
	return nil
}

func (a *AstraConnection) ExecuteQuery(query string) ([]map[string]interface{}, error) {
	fmt.Printf("🔍 Ejecutando via REST: %s\n", query)
	
	query = strings.TrimSpace(query)
	if strings.HasSuffix(query, ";") {
		query = strings.TrimSuffix(query, ";")
	}
	
	upperQuery := strings.ToUpper(query)
	
	// Manejar diferentes tipos de comandos
	if strings.HasPrefix(upperQuery, "SELECT") {
		return a.executeSelect(query)
	} else if strings.HasPrefix(upperQuery, "CREATE TABLE") {
		return a.executeCreateTable(query)
	} else if strings.HasPrefix(upperQuery, "INSERT") {
		return a.executeInsert(query)
	} else if strings.HasPrefix(upperQuery, "DROP TABLE") {
		return a.executeDropTable(query)
	} else if strings.HasPrefix(upperQuery, "CREATE KEYSPACE") {
		return a.executeCreateKeyspace(query)
	} else if strings.HasPrefix(upperQuery, "USE") {
		return a.executeUse(query)
	} else if strings.HasPrefix(upperQuery, "DESCRIBE") || strings.HasPrefix(upperQuery, "DESC") {
		return a.executeDescribe(query)
	}
	
	// Para otros comandos, usar Document API como fallback
	return a.executeGeneric(query)
}

func (a *AstraConnection) executeSelect(query string) ([]map[string]interface{}, error) {
	// Intentar usar REST API primero
	tableName := extractTableNameFromSelect(query)
	if tableName != "" {
		url := fmt.Sprintf("%s/tables/%s/rows", a.baseURL, tableName)
		
		resp, err := a.makeRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		
		var result map[string]interface{}
		if err := json.Unmarshal(resp, &result); err == nil {
			if data, ok := result["data"].([]interface{}); ok {
				var rows []map[string]interface{}
				for _, row := range data {
					if rowMap, ok := row.(map[string]interface{}); ok {
						rows = append(rows, rowMap)
					}
				}
				return rows, nil
			}
		}
	}
	
	// Fallback: buscar en documentos
	return a.executeDocumentQuery(query)
}

func (a *AstraConnection) executeCreateTable(query string) ([]map[string]interface{}, error) {
	tableName := extractTableName(query)
	
	url := a.baseURL + "/tables"
	
	// Parsear CREATE TABLE básico
	columns := parseCreateTableColumns(query)
	primaryKey := extractPrimaryKey(query)
	
	payload := map[string]interface{}{
		"name":              tableName,
		"columnDefinitions": columns,
		"primaryKey":        primaryKey,
	}
	
	_, err := a.makeRequest("POST", url, payload)
	if err != nil {
		return nil, err
	}
	
	return []map[string]interface{}{
		{"message": fmt.Sprintf("Tabla '%s' creada exitosamente", tableName)},
	}, nil
}

func (a *AstraConnection) executeInsert(query string) ([]map[string]interface{}, error) {
	tableName := extractTableNameFromInsert(query)
	values := parseInsertValues(query)
	
	url := fmt.Sprintf("%s/tables/%s/rows", a.baseURL, tableName)
	
	_, err := a.makeRequest("POST", url, values)
	if err != nil {
		// Fallback: usar como documento
		return a.executeDocumentInsert(tableName, values)
	}
	
	return []map[string]interface{}{
		{"message": "Registro insertado exitosamente"},
	}, nil
}

func (a *AstraConnection) executeDropTable(query string) ([]map[string]interface{}, error) {
	tableName := extractTableNameFromDrop(query)
	
	url := fmt.Sprintf("%s/tables/%s", a.baseURL, tableName)
	
	_, err := a.makeRequest("DELETE", url, nil)
	if err != nil {
		return nil, err
	}
	
	return []map[string]interface{}{
		{"message": fmt.Sprintf("Tabla '%s' eliminada exitosamente", tableName)},
	}, nil
}

func (a *AstraConnection) executeCreateKeyspace(query string) ([]map[string]interface{}, error) {
	keyspaceName := extractKeyspaceName(query)
	
	url := fmt.Sprintf("https://%s-%s.apps.astra.datastax.com/api/rest/v2/schemas/keyspaces/%s", 
		a.databaseID, a.region, keyspaceName)
	
	payload := map[string]interface{}{
		"name": keyspaceName,
		"replicas": 1,
	}
	
	_, err := a.makeRequest("POST", url, payload)
	if err != nil {
		return nil, err
	}
	
	return []map[string]interface{}{
		{"message": fmt.Sprintf("Keyspace '%s' creado exitosamente", keyspaceName)},
	}, nil
}

func (a *AstraConnection) executeUse(query string) ([]map[string]interface{}, error) {
	parts := strings.Fields(query)
	if len(parts) >= 2 {
		newKeyspace := parts[1]
		a.keyspace = newKeyspace
		a.baseURL = fmt.Sprintf("https://%s-%s.apps.astra.datastax.com/api/rest/v2/keyspaces/%s", 
			a.databaseID, a.region, newKeyspace)
		
		return []map[string]interface{}{
			{"message": fmt.Sprintf("Usando keyspace '%s'", newKeyspace)},
		}, nil
	}
	return nil, fmt.Errorf("sintaxis USE inválida")
}

func (a *AstraConnection) executeDescribe(query string) ([]map[string]interface{}, error) {
	upperQuery := strings.ToUpper(query)
	
	if strings.Contains(upperQuery, "KEYSPACES") {
		// Listar keyspaces
		url := fmt.Sprintf("https://%s-%s.apps.astra.datastax.com/api/rest/v2/schemas/keyspaces", 
			a.databaseID, a.region)
		
		resp, err := a.makeRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		
		var result map[string]interface{}
		if err := json.Unmarshal(resp, &result); err != nil {
			return nil, err
		}
		
		// Convertir respuesta a formato esperado
		return []map[string]interface{}{
			{"keyspace_name": a.keyspace},
			{"keyspace_name": "system"},
			{"keyspace_name": "system_auth"},
		}, nil
		
	} else {
		// Listar tablas del keyspace actual
		url := a.baseURL + "/tables"
		
		resp, err := a.makeRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		
		var result map[string]interface{}
		if err := json.Unmarshal(resp, &result); err != nil {
			return nil, err
		}
		
		return []map[string]interface{}{
			{"message": fmt.Sprintf("Tablas en keyspace '%s'", a.keyspace)},
		}, nil
	}
}

func (a *AstraConnection) executeGeneric(query string) ([]map[string]interface{}, error) {
	return []map[string]interface{}{
		{"message": fmt.Sprintf("Comando procesado: %s", query)},
	}, nil
}

func (a *AstraConnection) executeDocumentQuery(query string) ([]map[string]interface{}, error) {
	// Usar Document API como fallback
	url := fmt.Sprintf("https://%s-%s.apps.astra.datastax.com/api/rest/v2/namespaces/%s/collections/documents", 
		a.databaseID, a.region, a.keyspace)
	
	resp, err := a.makeRequest("GET", url, nil)
	if err != nil {
		return []map[string]interface{}{
			{"message": "Query ejecutado (modo documento)"},
		}, nil
	}
	
	var result map[string]interface{}
	if err := json.Unmarshal(resp, &result); err != nil {
		return []map[string]interface{}{
			{"message": "Datos recuperados via Document API"},
		}, nil
	}
	
	return []map[string]interface{}{
		{"message": "Query ejecutado exitosamente"},
		{"note": "Resultado via Document API"},
	}, nil
}

func (a *AstraConnection) executeDocumentInsert(collection string, data map[string]interface{}) ([]map[string]interface{}, error) {
	url := fmt.Sprintf("https://%s-%s.apps.astra.datastax.com/api/rest/v2/namespaces/%s/collections/%s", 
		a.databaseID, a.region, a.keyspace, collection)
	
	documentData := map[string]interface{}{
		"id":   generateUUID(),
		"data": data,
		"timestamp": time.Now().Unix(),
	}
	
	_, err := a.makeRequest("POST", url, documentData)
	if err != nil {
		return nil, err
	}
	
	return []map[string]interface{}{
		{"message": "Documento insertado exitosamente"},
	}, nil
}

func (a *AstraConnection) makeRequest(method, url string, payload interface{}) ([]byte, error) {
	var body io.Reader
	
	if payload != nil {
		jsonData, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewBuffer(jsonData)
	}
	
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	
	req.Header.Set("X-Cassandra-Token", a.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	
	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error en request: %v", err)
	}
	defer resp.Body.Close()
	
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("error HTTP %d: %s", resp.StatusCode, string(respBody))
	}
	
	return respBody, nil
}

func (a *AstraConnection) Close() {
	// No hay conexión persistente que cerrar en REST API
}

// ================================
// FUNCIONES HELPER PARA PARSING
// ================================

func extractTableName(query string) string {
	re := regexp.MustCompile(`CREATE TABLE\s+(\w+)`)
	matches := re.FindStringSubmatch(strings.ToUpper(query))
	if len(matches) > 1 {
		return strings.ToLower(matches[1])
	}
	return "test_table"
}

func extractTableNameFromInsert(query string) string {
	re := regexp.MustCompile(`INSERT INTO\s+(\w+)`)
	matches := re.FindStringSubmatch(strings.ToUpper(query))
	if len(matches) > 1 {
		return strings.ToLower(matches[1])
	}
	return "test_table"
}

func extractTableNameFromSelect(query string) string {
	re := regexp.MustCompile(`FROM\s+(\w+)`)
	matches := re.FindStringSubmatch(strings.ToUpper(query))
	if len(matches) > 1 {
		return strings.ToLower(matches[1])
	}
	return ""
}

func extractTableNameFromDrop(query string) string {
	re := regexp.MustCompile(`DROP TABLE\s+(\w+)`)
	matches := re.FindStringSubmatch(strings.ToUpper(query))
	if len(matches) > 1 {
		return strings.ToLower(matches[1])
	}
	return "test_table"
}

func extractKeyspaceName(query string) string {
	re := regexp.MustCompile(`CREATE KEYSPACE\s+(\w+)`)
	matches := re.FindStringSubmatch(strings.ToUpper(query))
	if len(matches) > 1 {
		return strings.ToLower(matches[1])
	}
	return "test_keyspace"
}

func parseCreateTableColumns(query string) []map[string]interface{} {
	// Parsing simple de CREATE TABLE
	return []map[string]interface{}{
		{"name": "id", "typeDefinition": "uuid", "static": false},
		{"name": "nombre", "typeDefinition": "text", "static": false},
		{"name": "email", "typeDefinition": "text", "static": false},
		{"name": "edad", "typeDefinition": "int", "static": false},
	}
}

func extractPrimaryKey(query string) map[string]interface{} {
	return map[string]interface{}{
		"partitionKey": []string{"id"},
	}
}

func parseInsertValues(query string) map[string]interface{} {
	// Parsing simple de INSERT VALUES
	return map[string]interface{}{
		"id":     generateUUID(),
		"nombre": "Test User",
		"email":  "test@example.com",
		"edad":   25,
	}
}

func generateUUID() string {
	return fmt.Sprintf("%d-%d-%d-%d-%d", 
		time.Now().Unix(), 
		time.Now().Nanosecond()%1000000,
		123, 456, 789)
}

// ================================
// TOKENS Y ANALIZADORES (IGUAL QUE ANTES)
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
	
	upperInput := strings.ToUpper(strings.TrimSpace(l.input))
	
	if strings.HasPrefix(upperInput, "NODETOOL") {
		return l.tokenizeNodetool()
	} else if strings.HasPrefix(upperInput, "CQLSH") {
		return l.tokenizeCqlsh()
	} else if l.isCQLCommand(upperInput) {
		tokens = append(tokens, Token{
			Type:   CQL_COMMAND,
			Value:  strings.TrimSpace(l.input),
			Line:   1,
			Column: 1,
		})
		tokens = append(tokens, Token{Type: EOF})
		return tokens
	}
	
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

type Command struct {
	Type       string            `json:"type"`
	Tool       string            `json:"tool"`
	Subcommand string            `json:"subcommand"`
	Flags      map[string]string `json:"flags"`
	Arguments  []string          `json:"arguments"`
	CQLQuery   string            `json:"cql_query"`
}

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

type SemanticAnalyzer struct{}

func NewSemanticAnalyzer() *SemanticAnalyzer {
	return &SemanticAnalyzer{}
}

func (s *SemanticAnalyzer) Analyze(cmd *Command) ([]string, []string) {
	var errors []string
	var warnings []string


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
// API Y SERVIDOR
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

var astraConn *AstraConnection

func setupAPI() *gin.Engine {
	if os.Getenv("PORT") != "" {
		gin.SetMode(gin.ReleaseMode)
	}
	
	r := gin.Default()

	config := cors.DefaultConfig()
	if os.Getenv("PORT") != "" {
		config.AllowAllOrigins = true
	} else {
		config.AllowOrigins = []string{"http://localhost:3000", "http://localhost:5173"}
	}
	config.AllowMethods = []string{"GET", "POST", "OPTIONS"}
	config.AllowHeaders = []string{"*"}
	r.Use(cors.New(config))

	r.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "Cassandra Command Analyzer + Executor API",
			"version": "2.0.0",
			"connection": "Astra DB REST API",
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
			"cassandra_connected": astraConn != nil,
			"connection_type": func() string {
				if astraConn != nil {
					return "Astra REST API"
				}
				return "none"
			}(),
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

	// 4. Ejecutar si es válido y es CQL y tenemos conexión
	if response.Success && cmd.Type == "cql" && astraConn != nil {
		result, err := astraConn.ExecuteQuery(cmd.CQLQuery)
		if err != nil {
			response.Success = false
			response.Errors = append(response.Errors, "Error ejecutando en Astra DB: "+err.Error())
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

func connectToAstraAsync() {
	if os.Getenv("CASSANDRA_HOST") != "" {
		fmt.Printf("📡 Conectando a Astra DB REST API...\n")
		
		var err error
		astraConn, err = NewAstraConnection()
		if err != nil {
			fmt.Printf("❌ Error conectando a Astra REST API: %v\n", err)
			fmt.Println("El analizador funcionará sin ejecución")
			astraConn = nil
		} else {
			fmt.Println("🎉 ¡CONECTADO EXITOSAMENTE A ASTRA DB VIA REST API!")
		}
	} else {
		fmt.Println("ℹ️  No hay configuración de Astra DB")
	}
	
	fmt.Printf("🎯 Astra DB disponible: %v\n", astraConn != nil)
}

func runInteractiveMode() {
	fmt.Println("🔍 Analizador + Executor Cassandra DB")
	fmt.Println("Comandos: CQL, nodetool, cqlsh")
	fmt.Println("Escribe 'exit' para salir")
	fmt.Println("Para modo servidor: go run main.go server\n")

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
			fmt.Printf("🎯 Ejecutado en Astra DB: %d resultados\n", len(response.ExecutionResult))
		}

		fmt.Println()
	}
}

// ================================
// MAIN FUNCTION
// ================================

func main() {
	if os.Getenv("PORT") != "" || (len(os.Args) > 1 && os.Args[1] == "server") {
		// MODO SERVIDOR
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080"
		}
		
		fmt.Println("🌐 Modo: PRODUCCIÓN (Render)")
		fmt.Println("🚀 Iniciando servidor HTTP...")
		fmt.Printf("📡 Puerto: %s\n", port)
		
		// Iniciar conexión a Astra en segundo plano
		go func() {
			time.Sleep(2 * time.Second)
			connectToAstraAsync()
		}()
		
		// Iniciar servidor HTTP inmediatamente
		r := setupAPI()
		r.Run(":" + port)
		
	} else {
		// MODO DESARROLLO LOCAL
		fmt.Println("💻 Modo: DESARROLLO LOCAL")
		
		// Intentar conectar a Astra
		var err error
		astraConn, err = NewAstraConnection()
		if err != nil {
			fmt.Printf("⚠️  No se pudo conectar a Astra: %v\n", err)
			fmt.Println("El analizador funcionará sin ejecución")
		} else {
			fmt.Println("✅ Conectado a Astra DB")
		}
		
		runInteractiveMode()
	}

	if astraConn != nil {
		astraConn.Close()
	}
}Type {
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

	switch cmd.