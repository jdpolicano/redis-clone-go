package main

import "fmt"

// --- Error Definitions ---

type ErrMissingPositional struct{}

func (e ErrMissingPositional) Error() string {
	return "arguments missing positional arg"
}

type ErrMissingNamedArg string

func (e ErrMissingNamedArg) Error() string {
	return fmt.Sprint("arguments missing named arg: ", string(e))
}

type ErrNamedArgMissingValue string

func (e ErrNamedArgMissingValue) Error() string {
	return fmt.Sprintf("named arg '%s' missing value", string(e))
}

type ErrUnknownArg string

func (e ErrUnknownArg) Error() string {
	return fmt.Sprint("encountered unknown arg: ", string(e))
}

type ErrDuplicatedArg string

func (e ErrDuplicatedArg) Error() string {
	return fmt.Sprint("encountered duplicate arg: ", string(e))
}

// --- Type Definitions ---

type ArgumentsParser struct {
	numPositionals int // positional arguments are required.
	argDefs        []ArgDef
}

type Arguments struct {
	Positionals []RespValue
	NamedArgs   []NamedArg
}

type ArgDef struct {
	name       string
	required   bool
	needsValue bool
}

type NamedArg struct {
	name  string
	value RespValue
}

// --- Constructors ---

func NewArgumentsParser() ArgumentsParser {
	return ArgumentsParser{0, nil}
}

func (parser ArgumentsParser) NumPositionals(i int) ArgumentsParser {
	parser.numPositionals = i
	return parser
}

func (parser ArgumentsParser) Argument(a ArgDef) ArgumentsParser {
	parser.argDefs = append(parser.argDefs, a)
	return parser
}

// --- Parsing Implementation ---

func (parser ArgumentsParser) Parse(raw []RespValue) (Arguments, error) {
	var args Arguments
	var noValue RespValue // noValue is a placeholder for flags that do not take a value

	// Validate that we have enough positional arguments.
	if len(raw) < parser.numPositionals {
		return args, ErrMissingPositional{}
	}

	// Slice out the positional arguments.
	args.Positionals = raw[:parser.numPositionals]
	raw = raw[parser.numPositionals:]

	// Process the remaining arguments as named arguments.
	// Use a helper map to track which named arguments have been provided,
	// so that duplicates are ignored (the first value is used).
	provided := make(map[string]bool)

	for i := 0; i < len(raw); i++ {
		argDef, err := parser.getArgDef(raw[i])
		if err != nil {
			return args, err
		}
		// If we have already seen this named argument, return an error
		if provided[argDef.name] {
			return args, ErrDuplicatedArg(argDef.name)
		}

		if argDef.needsValue {
			if i+1 >= len(raw) {
				return args, ErrNamedArgMissingValue(argDef.name)
			}
			args.NamedArgs = append(args.NamedArgs, NamedArg{argDef.name, raw[i+1]})
			provided[argDef.name] = true
			i++ // Skip the value that follows.
		} else {
			args.NamedArgs = append(args.NamedArgs, NamedArg{argDef.name, noValue})
			provided[argDef.name] = true
		}
	}

	// Verify that every required named argument is present.
	for _, def := range parser.argDefs {
		if def.required && !provided[def.name] {
			return args, ErrMissingNamedArg(def.name)
		}
	}

	return args, nil
}

// method to get an argument from the Arguments struct
func (a Arguments) GetArg(name string) (NamedArg, bool) {
	for _, na := range a.NamedArgs {
		if na.name == name {
			return na, true
		}
	}
	var none NamedArg
	return none, false
}

// method to get a postional from the Arguments struct
func (a Arguments) GetPos(i int) RespValue {
	return a.Positionals[i]
}

// Helper method to find the ArgDef matching a provided argument.
func (parser ArgumentsParser) getArgDef(search RespValue) (ArgDef, error) {
	var none ArgDef
	for _, argDef := range parser.argDefs {
		if search.EqualAsciiInsensitive(argDef.name) {
			return argDef, nil
		}
	}
	return none, ErrUnknownArg(search.String())
}
