package ringpop

import (
	"errors"
)

var (
	// ErrLabelCapacityExceeded is returned by Labels when the label cannot be
	// set because the label capacity has been exceeded.
	ErrLabelCapacityExceeded = errors.New("label capacity has been exceeded")
)

// Labels defines a way to interact with the labels local to this node. Label
// changes here will be synchronized with all the members of the cluster by the
// membership implementation and therefore be available at every instance of
// ringpop.
type Labels interface {
	// Get the value of a label for this instance of ringpop.
	Get(key string) (value string, has bool)

	// Set will set a certain label named by key to the value provided in value.
	// Set might return an error when the total capacity allocated to labels is
	// exhausted
	Set(key, value string) error

	// Remove will remove the label by the name of key and return wether the
	// label has been removed or not. If the label was not present before it
	// cannot be removed and thus will return false.
	Remove(key string) (removed bool)

	// AsMap exports all the local labels in a map[string]string. Changes to
	// this map will not be reflected on the Labels of this ringpop instance.
	AsMap() map[string]string
}
