package main

import (
	"fmt"
)

type ErrArgsOptionNotInt string

func (e ErrArgsOptionNotInt) Error() string {
	return fmt.Sprintf("argument '%s' was not passed as an integer", string(e))
}

type ErrArgNotFound string

func (e ErrArgNotFound) Error() string {
	return fmt.Sprintf("argument '%s' was not found", string(e))
}

type ErrRouteNotSupported string

func (e ErrRouteNotSupported) Error() string {
	return fmt.Sprintf("command '%s' does not have a valid handler function", string(e))
}

type ErrNoArgumentSupplied struct{}

func (e ErrNoArgumentSupplied) Error() string {
	return fmt.Sprintf("no argument provided with request")
}

type Callable func(ctx RequestContext, args []RespValue)

// will route a given request to the appropriate handler implementation
type CommandRouter struct {
	commands map[string]Command
}

type Command struct {
	Name string
	Call Callable
}

func NewCommandRouter() *CommandRouter {
	commands := make(map[string]Command)
	return &CommandRouter{commands}
}

func (cr *CommandRouter) Route(ctx RequestContext, args []RespValue) error {
	if len(args) < 1 {
		return ErrNoArgumentSupplied{}
	}

	name := args[0]
	args = args[1:]
	if cmd, supported := cr.commands[name.ToLower()]; supported {
		cmd.Call(ctx, args)
		return nil
	}

	ctx.SendError(ErrRouteNotSupported(name.String()).Error())
	return nil
}

func (cr *CommandRouter) Register(cmd Command) {
	cr.commands[cmd.Name] = cmd
}

func NewCommand(name string, caller Callable) Command {
	return Command{name, caller}
}

func (cmd Command) Matches(first RespValue) bool {
	if first.EqualAsciiInsensitive(cmd.Name) {
		return true
	}
	return false
}
