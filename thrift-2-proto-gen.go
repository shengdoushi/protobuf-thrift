package pbthrift

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/YYCoder/protobuf-thrift/utils"
	"github.com/YYCoder/protobuf-thrift/utils/logger"
	"github.com/YYCoder/thrifter"
)

type protoGenerator struct {
	conf                *ProtoGeneratorConfig
	def                 *thrifter.Thrift
	file                *os.File
	protoContent        bytes.Buffer
	thriftBridgeContent bytes.Buffer
	currentToken        *thrifter.Token
	packageDeclare      string // used to detect whether has duplicate package
	thriftRpcFuncs      []*thrifter.Function
	thriftServiceNames  []string
	thriftEnumNames     []string
}

type ProtoGeneratorConfig struct {
	taskType   int
	filePath   string // absolute path for current file
	fileName   string // output file name, including extension
	rawContent string
	outputDir  string // absolute path for output dir

	useSpaceIndent bool
	indentSpace    string
	fieldCase      string
	nameCase       string
	expSwitches    []string
	fixNamespace   string

	// pb config
	syntax             int // 2 or 3
	forceFieldOptional bool
	baseProtoFile      string
	baseProtoNs        string
	phpBridgeNs        string
}

func (c ProtoGeneratorConfig) getMixGenPhpNs() string {
	items := make([]string, 0)
	for _, v := range strings.Split(c.fixNamespace, ".") {
		items = append(items, utils.CaseConvert("upperFirstChar", v))
	}
	return "\\" + strings.Join(items, "\\")
}
func (c ProtoGeneratorConfig) HasSwitch(name string) bool {
	for _, s := range c.expSwitches {
		if s == name {
			return true
		}
	}
	return false
}

func (g *protoGenerator) checkIdentIsEnum(ident string) bool {
	for _, s := range g.thriftEnumNames {
		if s == ident {
			return true
		}
	}
	return false
}

func NewProtoGenerator(conf *ProtoGeneratorConfig) (res SubGenerator, err error) {
	var parser *thrifter.Parser
	var file *os.File
	var definition *thrifter.Thrift
	if conf.taskType == TASK_FILE_THRIFT2PROTO {
		file, err = os.Open(conf.filePath)
		if err != nil {
			return nil, err
		}
		defer file.Close()
		parser = thrifter.NewParser(file, false)
		definition, err = parser.Parse(file.Name())

	} else if conf.taskType == TASK_CONTENT_THRIFT2PROTO {
		rd := strings.NewReader(conf.rawContent)
		parser = thrifter.NewParser(rd, false)
		definition, err = parser.Parse("INPUT")
	}

	if err != nil {
		return
	}

	res = &protoGenerator{
		conf: conf,
		def:  definition,
		file: file,
	}
	return
}

func (g *protoGenerator) FilePath() (res string) {
	if g.conf.taskType == TASK_CONTENT_THRIFT2PROTO {
		res = ""
	} else {
		res = g.conf.filePath
	}
	return
}

func (g *protoGenerator) findNodeByStartToken(startToken *thrifter.Token, nodeType string) (res thrifter.Node) {
	for _, node := range g.def.Nodes {
		if node.NodeType() != nodeType {
			continue
		}
		switch node.NodeType() {
		case "Namespace":
			n := node.(*thrifter.Namespace)
			if n.StartToken == startToken {
				return n
			}
		case "Enum":
			n := node.(*thrifter.Enum)
			if n.StartToken == startToken {
				return n
			}
		case "Struct":
			n := node.(*thrifter.Struct)
			if n.StartToken == startToken {
				return n
			}
		case "Service":
			n := node.(*thrifter.Service)
			if n.StartToken == startToken {
				return n
			}
		case "Include":
			n := node.(*thrifter.Include)
			if n.StartToken == startToken {
				return n
			}
		}
	}
	return
}

// iterate over token linked-list until specified token.
func (g *protoGenerator) consumeUntilLiteral(lit string) {
	for g.currentToken.Raw != lit && g.currentToken != nil {
		g.currentToken = g.currentToken.Next
	}
}

// parse thrift ast, return absolute file paths included by current file
func (g *protoGenerator) Parse() (newFiles []FileInfo, err error) {
	g.handleSyntax()

	g.currentToken = g.def.StartToken

	for g.currentToken != nil {
		switch g.currentToken.Type {
		case thrifter.T_COMMENT:
			g.handleComment(g.currentToken)

		case thrifter.T_LINEBREAK, thrifter.T_RETURN:
			g.protoContent.WriteString(g.currentToken.Raw)
			g.currentToken = g.currentToken.Next

		case thrifter.T_NAMESPACE:
			n := g.findNodeByStartToken(g.currentToken, "Namespace")
			node := n.(*thrifter.Namespace)
			g.handleNamespace(node)
			g.currentToken = node.EndToken

		case thrifter.T_ENUM:
			n := g.findNodeByStartToken(g.currentToken, "Enum")
			node := n.(*thrifter.Enum)
			g.handleEnum(node)
			g.currentToken = node.EndToken

		case thrifter.T_STRUCT:
			n := g.findNodeByStartToken(g.currentToken, "Struct")
			node := n.(*thrifter.Struct)
			g.handleStruct(node)
			g.currentToken = node.EndToken

		case thrifter.T_SERVICE:
			n := g.findNodeByStartToken(g.currentToken, "Service")
			node := n.(*thrifter.Service)
			g.handleService(node)
			g.currentToken = node.EndToken

		case thrifter.T_INCLUDE, thrifter.T_CPP_INCLUDE:
			n := g.findNodeByStartToken(g.currentToken, "Include")
			node := n.(*thrifter.Include)
			newFiles = append(newFiles, g.handleIncludes(node.FilePath))
			g.currentToken = node.EndToken

		default:
			// other token will ignore
			g.currentToken = g.currentToken.Next
		}

	}

	return
}

func (g *protoGenerator) handleSyntax() {
	g.protoContent.WriteString(fmt.Sprintf("syntax = \"proto%d\";\n", g.conf.syntax))
	return
}

func (g *protoGenerator) handleNamespace(node *thrifter.Namespace) {
	if g.packageDeclare == "" {
		namespace := node.Value
		if g.conf.fixNamespace != "" {
			namespace = g.conf.fixNamespace
		}
		g.protoContent.WriteString(fmt.Sprintf("package %s;", namespace))
		g.packageDeclare = node.Value
		if g.conf.baseProtoFile != "" {
			g.protoContent.WriteString(fmt.Sprintf("\nimport \"%s\";\n", g.conf.baseProtoFile))
		}
	}

	return
}

func (g *protoGenerator) handleIncludes(path string) (newFile FileInfo) {
	if filepath.IsAbs(path) {
		relPath, err := filepath.Rel(g.conf.filePath, path)
		if err != nil {
			logger.Errorf("filepath.Rel %v %v, err %v", g.conf.filePath, path, err)
			return
		}
		newFile = FileInfo{
			absPath:    path,
			outputPath: filepath.Join(g.conf.outputDir, strings.ReplaceAll(relPath, ".thrift", ".proto")),
		}
	} else {
		newFile = FileInfo{
			absPath:    filepath.Join(filepath.Dir(g.conf.filePath), path),
			outputPath: filepath.Join(g.conf.outputDir, strings.ReplaceAll(path, ".thrift", ".proto")),
		}
	}

	filePath := strings.ReplaceAll(path, ".thrift", ".proto")
	// ! NOTE: proto import paths are relative to protoc command's working directory or using
	// ! NOTE: -I/--proto_path specified path, and can not include relative paths prefix, such as `./XXX.proto`.
	// ! NOTE: so, user have to manually check the generated path is correct.
	// ! NOTE: https://developers.google.com/protocol-buffers/docs/proto#importing_definitions
	g.protoContent.WriteString(fmt.Sprintf(`import "%s";`, filePath))
	return
}

// will ignore service/rpc options, since we already change to another language idl, the meaning for options are
// totally different
func (g *protoGenerator) handleService(s *thrifter.Service) {
	for g.currentToken != s.EndToken {
		switch g.currentToken.Type {
		case thrifter.T_COMMENT:
			g.writeIndent()
			g.handleComment(g.currentToken)

		case thrifter.T_LINEBREAK, thrifter.T_RETURN:
			g.protoContent.WriteString(g.currentToken.Raw)
			if g.conf.phpBridgeNs != "" {
				g.thriftBridgeContent.WriteString(g.currentToken.Raw)
			}
			g.currentToken = g.currentToken.Next

		case thrifter.T_SERVICE:
			g.consumeUntilLiteral("{")
			// consume { token
			g.currentToken = g.currentToken.Next
			g.protoContent.WriteString(fmt.Sprintf("service %s {", utils.CaseConvert(g.conf.nameCase, s.Ident)))
			if g.conf.phpBridgeNs != "" {
				g.thriftBridgeContent.WriteString(fmt.Sprintf("trait %sBridge {", utils.CaseConvert(g.conf.nameCase, s.Ident)))
				g.thriftBridgeContent.WriteString(fmt.Sprintf("\n\tabstract protected function getThriftServiceImpl() : \\%s\\%sIf;\n", strings.ReplaceAll(g.packageDeclare, ".", "\\"), s.Ident))
			}
			g.thriftServiceNames = append(g.thriftServiceNames, s.Ident)

		default:
			hash := thrifter.GenTokenHash(g.currentToken)
			// find out current token is the start token of a function node
			function, isFunctionStartToken := s.ElemsMap[hash]
			if !isFunctionStartToken {
				g.currentToken = g.currentToken.Next
				continue
			}

			name := utils.CaseConvert(g.conf.nameCase, function.Ident)
			// if there are multiple arguments, will only pick first one, because protobuf rpc only support one argument
			var reqName, resName string
			if len(function.Args) > 0 {
				// if the thrift function argument is a base type, e.g. i32/i64/bool/string, will be ignored
				reqName = utils.CaseConvert(g.conf.nameCase, function.Args[0].FieldType.Ident)
			}
			// if thrift function return type is void, leave the rpc return field empty
			if !function.Void && function.FunctionType != nil {
				resName = utils.CaseConvert(g.conf.nameCase, function.FunctionType.Ident)
			}
			// oneway/throws/options will be ignored.
			g.writeIndent()

			if g.conf.HasSwitch("gformat") {
				if !strings.HasSuffix(resName, "Request") {
					reqName = utils.CaseConvert("upperFirstChar", name+"Request")
				}
				if !strings.HasSuffix(resName, "Response") {
					resName = utils.CaseConvert("upperFirstChar", name+"Response")
				}
			}
			g.protoContent.WriteString(fmt.Sprintf("rpc %s(%s) returns (%s) {}", name, reqName, resName))

			if g.conf.phpBridgeNs != "" {
				g.thriftBridgeContent.WriteString(fmt.Sprintf("\n\tpublic function %s(\\Mix\\Grpc\\Context $context, %s $request) : %s\n", name, reqName, resName))
				g.thriftBridgeContent.WriteString("\t{\n")
				var bridgeFuncArgs []string
				for _, arg := range function.Args {
					if arg.FieldType.Type == thrifter.FIELD_TYPE_BASE {
						bridgeFuncArgs = append(bridgeFuncArgs, fmt.Sprintf("$request->get%s()", utils.CaseConvert("upperFirstChar", arg.Ident)))
					} else if arg.FieldType.Type == thrifter.FIELD_TYPE_IDENT {
						if g.checkIdentIsEnum(arg.FieldType.Ident) {
							bridgeFuncArgs = append(bridgeFuncArgs, fmt.Sprintf("$request->get%s()", utils.CaseConvert("upperFirstChar", arg.Ident)))
						} else {
							bridgeFuncArgs = append(bridgeFuncArgs, fmt.Sprintf("new \\%s\\%s(json_decode($request->get%s()->serializeToJsonString(), true))", strings.ReplaceAll(g.packageDeclare, ".", "\\"), utils.CaseConvert(g.conf.nameCase, arg.FieldType.Ident), utils.CaseConvert("upperFirstChar", arg.Ident)))
						}
					} else if arg.FieldType.Type == thrifter.FIELD_TYPE_LIST {
						if arg.FieldType.List.Elem.Type == thrifter.FIELD_TYPE_BASE {
							bridgeFuncArgs = append(bridgeFuncArgs, fmt.Sprintf("iterator_to_array($request->get%s())", utils.CaseConvert("upperFirstChar", arg.Ident)))
						} else if arg.FieldType.List.Elem.Type == thrifter.FIELD_TYPE_IDENT {
							if g.checkIdentIsEnum(arg.FieldType.List.Elem.Ident) {
								bridgeFuncArgs = append(bridgeFuncArgs, fmt.Sprintf("iterator_to_array($request->get%s())", utils.CaseConvert("upperFirstChar", arg.Ident)))
							} else {
								bridgeFuncArgs = append(bridgeFuncArgs, fmt.Sprintf("array_map(fn ($item) => new \\%s\\%s(json_decode($item->serializeToJsonString(), true)), iterator_to_array($request->get%s()))", strings.ReplaceAll(g.packageDeclare, ".", "\\"), utils.CaseConvert(g.conf.nameCase, arg.FieldType.List.Elem.Ident), utils.CaseConvert("upperFirstChar", arg.Ident)))
							}
						} else {
							panic(fmt.Sprintf("不支持的 bridge list<value> %d 参数类型， 请升级工具", arg.FieldType.List.Elem.Type))
						}
					} else if arg.FieldType.Type == thrifter.FIELD_TYPE_MAP {
						if arg.FieldType.Map.Key.Type == thrifter.FIELD_TYPE_BASE && arg.FieldType.Map.Value.Type == thrifter.FIELD_TYPE_BASE {
							bridgeFuncArgs = append(bridgeFuncArgs, fmt.Sprintf("iterator_to_array($request->get%s())", utils.CaseConvert("upperFirstChar", arg.Ident)))
						} else {
							panic(fmt.Sprintf("不支持的 bridge map<key, value> 参数类型， 请升级工具"))
						}
					} else {
						panic(fmt.Sprintf("不支持的 bridge %d 参数类型， 请升级工具", arg.FieldType.Type))
					}
				}
				g.thriftBridgeContent.WriteString(fmt.Sprintf("\t\t$result = $this->getThriftServiceImpl()->%s(%s);\n", name, strings.Join(bridgeFuncArgs, ", ")))
				if function.Void || function.FunctionType == nil {
					g.thriftBridgeContent.WriteString(fmt.Sprintf("\t\treturn (new %s());\n", resName))
				} else {
					bridgeReturnValue := "TODO"
					if function.FunctionType.Type == thrifter.FIELD_TYPE_BASE {
						bridgeReturnValue = "$result"
					} else if function.FunctionType.Type == thrifter.FIELD_TYPE_IDENT {
						if g.checkIdentIsEnum(function.FunctionType.Ident) {
							bridgeReturnValue = "$result"
						} else {
							bridgeReturnValue = fmt.Sprintf("new %s\\%s(array_filter((array)$result, fn ($item) => !is_null($item)))", g.conf.getMixGenPhpNs(), utils.CaseConvert(g.conf.nameCase, function.FunctionType.Ident))
						}
					} else if function.FunctionType.Type == thrifter.FIELD_TYPE_LIST {
						if function.FunctionType.List.Elem.Type == thrifter.FIELD_TYPE_BASE {
							bridgeReturnValue = fmt.Sprintf("$result")
						} else if function.FunctionType.List.Elem.Type == thrifter.FIELD_TYPE_IDENT {
							if g.checkIdentIsEnum(function.FunctionType.List.Elem.Ident) {
								bridgeReturnValue = fmt.Sprintf("$result")
							} else {
								bridgeReturnValue = fmt.Sprintf("array_map(fn ($item) => new %s\\%s(array_filter((array)$item, fn ($item) => !is_null($item))), $result)", g.conf.getMixGenPhpNs(), utils.CaseConvert(g.conf.nameCase, function.FunctionType.List.Elem.Ident))
							}
						} else if function.FunctionType.List.Elem.Type == thrifter.FIELD_TYPE_MAP {
							if function.FunctionType.List.Elem.Map.Key.Type == thrifter.FIELD_TYPE_BASE && function.FunctionType.List.Elem.Map.Value.Type == thrifter.FIELD_TYPE_BASE {
								items := strings.Split(g.conf.baseProtoNs, ".")
								for idx := range items {
									items[idx] = utils.CaseConvert("upperFirstChar", items[idx])
								}
								selfMapMsgType := strings.Join(items, "\\") + "\\" + "Map" + utils.CaseConvert("upperFirstChar", function.FunctionType.List.Elem.Map.Key.BaseType) + "To" + utils.CaseConvert("upperFirstChar", function.FunctionType.List.Elem.Map.Key.BaseType)
								bridgeReturnValue = fmt.Sprintf("array_map(fn ($item) => new %s($item), $result)", selfMapMsgType)
							} else {
								panic(fmt.Sprintf("不支持的 bridge list<map<key, value>> %d, %d 返回类型， 请升级工具", function.FunctionType.List.Elem.Map.Key.Type, function.FunctionType.List.Elem.Map.Value.Type))
							}
						} else {
							panic(fmt.Sprintf("不支持的 bridge list<value> %d 返回类型， 请升级工具", function.FunctionType.List.Elem.Type))
						}
					} else if function.FunctionType.Type == thrifter.FIELD_TYPE_MAP {
						if function.FunctionType.Map.Key.Type == thrifter.FIELD_TYPE_BASE && function.FunctionType.Map.Value.Type == thrifter.FIELD_TYPE_BASE {
							bridgeReturnValue = fmt.Sprintf("$result")
						} else {
							panic(fmt.Sprintf("不支持的 bridge map<key, value> 返回类型， 请升级工具"))
						}
					} else {
						panic(fmt.Sprintf("不支持的 bridge %d 返回类型， 请升级工具", function.FunctionType.Type))
					}
					g.thriftBridgeContent.WriteString(fmt.Sprintf("\t\treturn (new %s\\%s())->setValue(%s);\n", g.conf.getMixGenPhpNs(), resName, bridgeReturnValue))
				}
				g.thriftBridgeContent.WriteString("\t}")
			}

			g.thriftRpcFuncs = append(g.thriftRpcFuncs, function)
			// move to end token of the function node
			g.currentToken = function.EndToken
		}
	}

	g.protoContent.WriteString("}\n")
	if g.conf.phpBridgeNs != "" {
		g.thriftBridgeContent.WriteString("}\n")
	}
	g.currentToken = s.EndToken

	return
}

func (g *protoGenerator) handleComment(tok *thrifter.Token) {
	if strings.HasPrefix(g.currentToken.Raw, "#") {
		content := fmt.Sprintf("//%s", strings.Replace(g.currentToken.Raw, "#", "", 1))
		g.protoContent.WriteString(content)
		g.currentToken = g.currentToken.Next
		return
	}
	g.protoContent.WriteString(g.currentToken.Raw)
	g.currentToken = g.currentToken.Next
}

func (g *protoGenerator) handleEnum(e *thrifter.Enum) {
	hasTraverseFirstElement := false
	lastEnumValue := -1

	for g.currentToken != e.EndToken {
		switch g.currentToken.Type {
		case thrifter.T_COMMENT:
			g.writeIndent()
			g.handleComment(g.currentToken)

		case thrifter.T_LINEBREAK, thrifter.T_RETURN:
			g.protoContent.WriteString(g.currentToken.Raw)
			g.currentToken = g.currentToken.Next

		case thrifter.T_ENUM:
			g.consumeUntilLiteral("{")
			// consume { token
			g.currentToken = g.currentToken.Next
			g.protoContent.WriteString(fmt.Sprintf("enum %s {", utils.CaseConvert(g.conf.nameCase, e.Ident)))
			g.thriftEnumNames = append(g.thriftEnumNames, e.Ident)

		default:
			hash := thrifter.GenTokenHash(g.currentToken)
			// find out current token is the start token of a enum element node
			ele, isElemStartToken := e.ElemsMap[hash]
			if !isElemStartToken {
				g.currentToken = g.currentToken.Next
				continue
			}

			if !hasTraverseFirstElement {
				hasTraverseFirstElement = true
				// proto 3 enum first element must be zero, add a default element to it
				if ele.ID > 0 && g.conf.syntax == 3 {
					g.writeIndent()
					name := utils.CaseConvert(g.conf.fieldCase, fmt.Sprintf("%s_Unknown", e.Ident))
					g.protoContent.WriteString(fmt.Sprintf("%s = 0;\n", name))
				}
			}
			name := utils.CaseConvert(g.conf.fieldCase, ele.Ident)
			g.writeIndent()
			if ele.ID != 0 {
				lastEnumValue = ele.ID
			} else {
				lastEnumValue++
			}
			g.protoContent.WriteString(fmt.Sprintf("%s_%s = %d;", e.Ident, name, lastEnumValue))

			if g.currentToken == ele.EndToken {
				g.currentToken = g.currentToken.Next
				continue
			}
			// move to end token of the enum element node
			g.currentToken = ele.EndToken
		}
	}

	g.protoContent.WriteString("}")
	g.currentToken = e.EndToken

	return
}

func (g *protoGenerator) handleStruct(s *thrifter.Struct) {
	for g.currentToken != s.EndToken {
		switch g.currentToken.Type {
		case thrifter.T_COMMENT:
			g.writeIndent()
			g.handleComment(g.currentToken)

		case thrifter.T_LINEBREAK, thrifter.T_RETURN:
			g.protoContent.WriteString(g.currentToken.Raw)
			g.currentToken = g.currentToken.Next

		case thrifter.T_STRUCT:
			g.consumeUntilLiteral("{")
			// consume { token
			g.currentToken = g.currentToken.Next
			g.protoContent.WriteString(fmt.Sprintf("message %s {", utils.CaseConvert(g.conf.nameCase, s.Ident)))

		default:
			hash := thrifter.GenTokenHash(g.currentToken)
			// find out current token is the start token of a struct element node
			ele, isElemStartToken := s.ElemsMap[hash]
			if !isElemStartToken {
				g.currentToken = g.currentToken.Next
				continue
			}

			name := utils.CaseConvert(g.conf.fieldCase, ele.Ident)

			switch ele.FieldType.Type {
			// set would be list
			case thrifter.FIELD_TYPE_LIST, thrifter.FIELD_TYPE_SET:
				// TODO: support nested list/set/map
				typeNameOrIdent := ""
				if ele.FieldType.List.Elem.Type == thrifter.FIELD_TYPE_BASE {
					typeNameOrIdent = ele.FieldType.List.Elem.BaseType
				} else {
					typeNameOrIdent = ele.FieldType.List.Elem.Ident
				}
				fieldType, _ := g.typeConverter(typeNameOrIdent)

				g.writeIndent()
				g.protoContent.WriteString(fmt.Sprintf("repeated %s %s = %d;", fieldType, name, ele.ID))

			case thrifter.FIELD_TYPE_MAP:
				optional := g.conf.syntax == 2 && ele.Requiredness == "optional"
				fieldType, keyType := "", ""
				// TODO: support nested types for map value
				if ele.FieldType.Map.Value.Type == thrifter.FIELD_TYPE_BASE {
					fieldType, _ = g.typeConverter(ele.FieldType.Map.Value.BaseType)
				} else {
					fieldType, _ = g.typeConverter(ele.FieldType.Map.Value.Ident)
				}
				if ele.FieldType.Map.Key.Type == thrifter.FIELD_TYPE_BASE {
					keyType, _ = g.typeConverter(ele.FieldType.Map.Key.BaseType)
				} else {
					keyType, _ = g.typeConverter(ele.FieldType.Map.Key.Ident)
				}
				g.writeIndent()
				if optional {
					g.protoContent.WriteString("optional ")
				}
				g.protoContent.WriteString(fmt.Sprintf("map<%s, %s> %s = %d;", keyType, fieldType, name, ele.ID))

			default:
				optional := g.conf.syntax == 2 && ele.Requiredness == "optional" || g.conf.forceFieldOptional
				typeNameOrIdent := ""
				if ele.FieldType.Type == thrifter.FIELD_TYPE_BASE {
					typeNameOrIdent = ele.FieldType.BaseType
				} else {
					typeNameOrIdent = ele.FieldType.Ident
				}
				fieldType, _ := g.typeConverter(typeNameOrIdent)

				g.writeIndent()
				if optional {
					g.protoContent.WriteString("optional ")
				}

				g.protoContent.WriteString(fmt.Sprintf("%s %s = %d;", fieldType, name, ele.ID))
			}

			// move to end token of the enum element node
			g.currentToken = ele.EndToken
		}
	}

	g.protoContent.WriteString("}")
	g.currentToken = s.EndToken
	return
}

func (g *protoGenerator) typeConverter(t string) (res string, err error) {
	res, err = g.basicTypeConverter(t)
	if err != nil {
		// if t is not a basic type, then we should convert its case, same as name
		res = utils.CaseConvert(g.conf.nameCase, t)
		return res, nil
	}
	return
}

func (g *protoGenerator) basicTypeConverter(t string) (res string, err error) {
	switch t {
	case "string":
		res = "string"
	case "i64":
		res = "int64"
	case "i32":
		res = "int32"
	case "double":
		res = "double"
	case "bool":
		res = "bool"
	// case "byte":
	// 	res = "bytes"
	default:
		if g.conf.HasSwitch("i16ToInt32") && t == "i16" {
			res = "int32"
			return
		}
		err = fmt.Errorf("Invalid basic type %s", t)
	}
	return
}
func (g *protoGenerator) writeFunctionArgs(args []*thrifter.Field) {
	for _, ele := range args {
		name := utils.CaseConvert(g.conf.fieldCase, ele.Ident)
		switch ele.FieldType.Type {
		// set would be list
		case thrifter.FIELD_TYPE_LIST, thrifter.FIELD_TYPE_SET:
			// TODO: support nested list/set/map
			typeNameOrIdent := ""
			if ele.FieldType.List.Elem.Type == thrifter.FIELD_TYPE_BASE {
				typeNameOrIdent = ele.FieldType.List.Elem.BaseType
			} else {
				typeNameOrIdent = ele.FieldType.List.Elem.Ident
			}
			fieldType, _ := g.typeConverter(typeNameOrIdent)

			g.writeIndent()
			if fieldType == "" && g.conf.baseProtoFile != "" {
				// list<map> 从 base 中定义
				if ele.FieldType.List.Elem.Type == thrifter.FIELD_TYPE_MAP {
					keyType := ele.FieldType.List.Elem.Map.Key.BaseType
					valueType := ele.FieldType.List.Elem.Map.Key.BaseType

					fieldType = g.conf.baseProtoNs + "." + "Map" + utils.CaseConvert("upperFirstChar", keyType) + "To" + utils.CaseConvert("upperFirstChar", valueType)
				}
			}
			g.protoContent.WriteString(fmt.Sprintf("repeated %s %s = %d;", fieldType, name, ele.ID))

		case thrifter.FIELD_TYPE_MAP:
			optional := g.conf.syntax == 2 && ele.Requiredness == "optional"
			fieldType, keyType := "", ""
			// TODO: support nested types for map value
			if ele.FieldType.Map.Value.Type == thrifter.FIELD_TYPE_BASE {
				fieldType, _ = g.typeConverter(ele.FieldType.Map.Value.BaseType)
			} else {
				fieldType, _ = g.typeConverter(ele.FieldType.Map.Value.Ident)
			}
			if ele.FieldType.Map.Key.Type == thrifter.FIELD_TYPE_BASE {
				keyType, _ = g.typeConverter(ele.FieldType.Map.Key.BaseType)
			} else {
				keyType, _ = g.typeConverter(ele.FieldType.Map.Key.Ident)
			}
			g.writeIndent()
			if optional {
				g.protoContent.WriteString("optional ")
			}
			g.protoContent.WriteString(fmt.Sprintf("map<%s, %s> %s = %d;", keyType, fieldType, name, ele.ID))

		default:
			optional := g.conf.syntax == 2 && ele.Requiredness == "optional" || g.conf.forceFieldOptional
			typeNameOrIdent := ""
			if ele.FieldType.Type == thrifter.FIELD_TYPE_BASE {
				typeNameOrIdent = ele.FieldType.BaseType
			} else {
				typeNameOrIdent = ele.FieldType.Ident
			}
			fieldType, _ := g.typeConverter(typeNameOrIdent)

			g.writeIndent()
			if optional {
				g.protoContent.WriteString("optional ")
			}

			g.protoContent.WriteString(fmt.Sprintf("%s %s = %d;", fieldType, name, ele.ID))
		}
		g.protoContent.WriteString("\n")

	}
}

// write thrift code from thriftAST to output
func (g *protoGenerator) Sink() (err error) {

	if g.conf.HasSwitch("gformat") {
		for _, rpcFun := range g.thriftRpcFuncs {
			reqName := utils.CaseConvert("upperFirstChar", utils.CaseConvert(g.conf.nameCase, rpcFun.Ident)+"Request")
			if len(rpcFun.Args) != 1 || utils.CaseConvert(g.conf.nameCase, rpcFun.Args[0].Ident) != reqName {
				g.protoContent.WriteString(fmt.Sprintf("message %s {\n", reqName))
				g.writeFunctionArgs(rpcFun.Args)
				g.protoContent.WriteString("\n}\n\n")
			}

			resName := utils.CaseConvert("upperFirstChar", utils.CaseConvert(g.conf.nameCase, rpcFun.Ident)+"Response")
			if rpcFun.FunctionType == nil || utils.CaseConvert(g.conf.nameCase, rpcFun.FunctionType.Ident) != resName {
				g.protoContent.WriteString(fmt.Sprintf("message %s {\n", resName))
				if rpcFun.FunctionType != nil {
					g.writeFunctionArgs([]*thrifter.Field{
						&thrifter.Field{
							FieldType: rpcFun.FunctionType,
							Ident:     "value",
							ID:        1,
						},
					})
				}
				g.protoContent.WriteString("\n}\n\n")
			}
		}
	}

	if g.conf.outputDir != "" {
		var file *os.File
		err = os.MkdirAll(g.conf.outputDir, 0755)
		if err != nil {
			logger.Errorf("Error occurred when MkdirAll %v", g.conf.outputDir)
			return
		}
		outputPath := filepath.Join(g.conf.outputDir, g.conf.fileName)
		file, err = os.Create(outputPath)
		if err != nil {
			logger.Errorf("os.Create file %v error %v", outputPath, err)
			return err
		}
		defer file.Close()
		_, err = file.WriteString(g.protoContent.String())

		if g.conf.phpBridgeNs != "" && len(g.thriftServiceNames) > 0 {
			outputPath = g.conf.outputDir
			for _, v := range strings.Split(strings.TrimLeft(g.conf.phpBridgeNs, "\\"), "\\")[1:] {
				outputPath = filepath.Join(outputPath, v)
			}
			os.MkdirAll(outputPath, os.ModePerm)
			outputPath = filepath.Join(outputPath, utils.CaseConvert(g.conf.nameCase, g.thriftServiceNames[0])+"Bridge.php")
			file, err = os.Create(outputPath)
			if err != nil {
				logger.Errorf("os.Create file %v error %v", outputPath, err)
				return err
			}
			defer file.Close()
			file.WriteString("<?php\n\n")
			file.WriteString(fmt.Sprintf("namespace %s;\n", g.conf.phpBridgeNs))
			file.WriteString("/**\n * Autogenerated by Protobuf-Thrift Tool\n *\n * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING\n *  @generated\n */\n\n")
			for _, rpcFun := range g.thriftRpcFuncs {
				reqName := utils.CaseConvert("upperFirstChar", utils.CaseConvert(g.conf.nameCase, rpcFun.Ident)+"Request")
				resName := utils.CaseConvert("upperFirstChar", utils.CaseConvert(g.conf.nameCase, rpcFun.Ident)+"Response")
				file.WriteString(fmt.Sprintf("use %s\\%s;\n", g.conf.getMixGenPhpNs(), reqName))
				file.WriteString(fmt.Sprintf("use %s\\%s;\n", g.conf.getMixGenPhpNs(), resName))
			}
			_, err = file.WriteString(g.thriftBridgeContent.String())
		}
	} else {
		f := bufio.NewWriter(os.Stdout)
		defer f.Flush()
		_, err = f.Write(g.protoContent.Bytes())
	}

	return
}

func (g *protoGenerator) writeIndent() {
	if g.conf.useSpaceIndent {
		spaceCount, _ := strconv.Atoi(g.conf.indentSpace)
		for i := 0; i < spaceCount; i++ {
			g.protoContent.WriteString(" ")
		}
	} else {
		g.protoContent.WriteString("	")
	}
	return
}

func (g *protoGenerator) Pipe() (res []byte, err error) {
	return g.protoContent.Bytes(), nil
}
