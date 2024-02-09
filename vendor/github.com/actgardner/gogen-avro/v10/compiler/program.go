package compiler

import (
	"fmt"

	"github.com/actgardner/gogen-avro/v10/vm"
)

// Build an intermediate representation of the program where
// methods, loops, switches, etc. are represented logically.
// Then concatenate everything together and replace the flow
// control with jumps to absolute offsets.

type irProgram struct {
	main          *irMethod
	methods       map[string]*irMethod
	blocks        []*irBlock
	switches      []*irSwitch
	errors        []string
	allowLaxNames bool
}

type irBlock struct {
	start int
	end   int
}

func (b *irBlock) String() string {
	return fmt.Sprintf("%v - %v", b.start, b.end)
}

type irSwitch struct {
	start int
	cases map[int]int
	end   int
}

func (b *irSwitch) String() string {
	return fmt.Sprintf("%v - %v - %v", b.start, b.cases, b.end)
}

func (p *irProgram) createMethod(name string) *irMethod {
	method := newIRMethod(name, p)
	p.methods[name] = method
	return method
}

// Concatenate all the IR instructions and assign them absolute offsets.
// An IR instruction maps to a fixed number of VM instructions,
// So we track the length of the finished output to get the real offsets.
// Main ends with a halt(0), everything else ends with a return.
func (p *irProgram) CompileToVM() (*vm.Program, error) {
	irProgram := make([]irInstruction, 0)
	vmLength := 0

	p.main.addLiteral(vm.Halt, 0)
	vmLength += p.main.VMLength()
	irProgram = append(irProgram, p.main.body...)

	for _, method := range p.methods {
		method.offset = vmLength
		method.addLiteral(vm.Return, vm.NoopField)
		vmLength += method.VMLength()
		irProgram = append(irProgram, method.body...)
	}

	p.findOffsets(irProgram)
	log("Found blocks: %v", p.blocks)

	vmProgram := make([]vm.Instruction, 0)
	for _, instruction := range irProgram {
		compiled, err := instruction.CompileToVM(p)
		if err != nil {
			return nil, err
		}
		vmProgram = append(vmProgram, compiled...)
	}
	return &vm.Program{
		Instructions: vmProgram,
		Errors:       p.errors,
	}, nil
}

func (p *irProgram) findOffsets(inst []irInstruction) {
	offset := 0
	for _, instruction := range inst {
		switch v := instruction.(type) {
		case *blockStartIRInstruction:
			log("findOffsets() block %v - start %v", v.blockId, offset)
			p.blocks[v.blockId].start = offset
		case *blockEndIRInstruction:
			log("findOffsets() block %v - end %v", v.blockId, offset)
			p.blocks[v.blockId].end = offset
		case *switchStartIRInstruction:
			log("findOffsets() block %v - start %v", v.switchId, offset)
			p.switches[v.switchId].start = offset
		case *switchCaseIRInstruction:
			log("findOffsets() block %v - start %v", v.switchId, offset)
			p.switches[v.switchId].cases[v.writerIndex] = offset
		case *switchEndIRInstruction:
			log("findOffsets() block %v - end %v", v.switchId, offset)
			p.switches[v.switchId].end = offset
		}
		offset += instruction.VMLength()
	}
}
