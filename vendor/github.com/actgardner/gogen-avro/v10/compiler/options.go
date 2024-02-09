package compiler

type Option func(*irProgram)

func AllowLaxNames() Option {
	return func(program *irProgram) {
		program.allowLaxNames = true
	}
}
