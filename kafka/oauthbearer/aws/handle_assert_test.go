package aws

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

// Compile-time assertions that Producer, Consumer, and AdminClient all
// satisfy the Handle interface. If a future kafka package refactor silently
// breaks this contract, these lines will stop compiling — a deliberate
// tripwire so a behavioural bug can't slip through.
var (
	_ Handle = (*kafka.Producer)(nil)
	_ Handle = (*kafka.Consumer)(nil)
	_ Handle = (*kafka.AdminClient)(nil)
)
