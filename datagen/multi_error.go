package datagen

import (
	"fmt"
	"strings"
)

type multiError struct {
	errs []error
}

func (e *multiError) Error() string {
	messages := []string{}
	for _, err := range e.errs {
		messages = append(messages, err.Error())
	}
	joinedMessage := strings.Join(messages, "\n")
	return fmt.Sprintf("multiple errors (%d):\n%s", len(e.errs), joinedMessage)
}
