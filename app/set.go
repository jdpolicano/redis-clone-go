package main

import (
	"strconv"
	"time"
)

var SetCommand = Command{"set", set}

var SetArgsParser = NewArgumentsParser().
	NumPositionals(2).
	Argument(ArgDef{"PX", false, true})

func set(ctx RequestContext, args []RespValue) {
	parsedArgs, e := SetArgsParser.Parse(args)
	if e != nil {
		ctx.SendError(e.Error())
		return
	}
	key, value := parsedArgs.GetPos(0), parsedArgs.GetPos(1)

	if px, exists := parsedArgs.GetArg("PX"); exists {
		milli, e := strconv.Atoi(px.value.String())
		if e != nil {
			ctx.SendError(e.Error())
			return
		}
		ctx.DB.Set(key.String(), value, time.Duration(milli)*time.Millisecond)
		ctx.SendSimpleString("OK")
		return
	}

	ctx.DB.Set(key.String(), value, time.Duration(0))
	ctx.SendSimpleString("OK")
}
