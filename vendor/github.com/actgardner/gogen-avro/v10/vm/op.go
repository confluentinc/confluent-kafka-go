package vm

// OP represents an opcode for the VM. Operations take 0 or 1 operands.
type Op int

const (
	// Read a value of the operand type from the wire and put itin the frame
	Read Op = iota

	// Set the current target to the value of the operand type from the frame
	Set

	// Allocate a new frame and make the target the field with the operand index
	Enter

	// Move to the previous frame
	Exit

	// Set a flag to null this field on exit
	SetExitNull

	// Append a value to the current target and enter the new value
	AppendArray

	// Append a new key-value pair (where the key is the String value in the current frame) to the current target and enter the new value
	AppendMap

	// Set the value of the field at the operand index to it's default value
	SetDefault

	// Push the current address onto the call stack and move the PC to the operand address
	Call

	// Pop the top value frmm the call stack and set the PC to that address
	Return

	// Stop the VM. If the operand is greater than zero, look up the corresponding error message and return it
	Halt

	// Move the PC to the operand
	Jump

	// Evaluate whether the Long register is equal to the operand, and set the condition register to the result
	EvalEqual

	// Evaluate whether the Long register is greater than the operand, and set the condition register to the result
	EvalGreater

	// If the condition register is true, jump to the operand instruction
	CondJump

	// Set the Long register to the operand value
	SetLong

	// Add the operand value to the Long register
	AddLong

	// Multiply the operand value by the Long register
	MultLong

	// Set the Int register to the operand value
	SetInt

	// Push the current Long register value onto the loop stack
	PushLoop

	// Pop the top of the loop stack and store the value in the Long register
	PopLoop

	// Set the field with the target index to nil
	NullField

	// Hint at the final size of a map or an array for performance
	HintSize
)

// String implements Stringer interface.
func (o Op) String() string {
	switch o {
	case Read:
		return "read"
	case Set:
		return "set"
	case Enter:
		return "enter"
	case Exit:
		return "exit"
	case SetExitNull:
		return "set_exit_null"
	case AppendArray:
		return "append_array"
	case AppendMap:
		return "append_map"
	case Call:
		return "call"
	case Return:
		return "return"
	case Halt:
		return "halt"
	case Jump:
		return "jump"
	case EvalEqual:
		return "eval_equal"
	case EvalGreater:
		return "eval_greater"
	case CondJump:
		return "cond_jump"
	case AddLong:
		return "add_long"
	case MultLong:
		return "mult_long"
	case SetDefault:
		return "set_def"
	case PushLoop:
		return "push_loop"
	case PopLoop:
		return "pop_loop"
	case SetLong:
		return "set_long"
	case SetInt:
		return "set_int"
	case NullField:
		return "null_field"
	case HintSize:
		return "hint_size"
	}
	return "Unknown"
}
