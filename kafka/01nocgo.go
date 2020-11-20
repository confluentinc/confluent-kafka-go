// +build !cgo

package kafka

///////////////////////////////////////////////////////////////////////////////
// Cross-compilation is not supported, you must build your Linux application //
// on a Linux machine or inside a Linux docker container.                    //
//                                                                           //
// There is no workaround for building an OSX application on Linux.          //
//                                                                           //
//                                                                           //
// CGO_ENABLED must be enabled, which it is by default unless GOOS is set.   //
///////////////////////////////////////////////////////////////////////////////
import (
	"ConfluentKafkaGo_CANT_BE_CROSS_COMPILED"
	"ConfluentKafkaGo_REQUIRES_CGO_ENABLED"
)
