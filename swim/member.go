// Copyright (c) 2015 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package swim

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"

	"github.com/dgryski/go-farm"
	"github.com/uber/ringpop-go/util"
)

const (
	// Alive is the member "alive" state
	Alive = "alive"

	// Suspect is the member "suspect" state
	Suspect = "suspect"

	// Faulty is the member "faulty" state
	Faulty = "faulty"

	// Leave is the member "leave" state
	Leave = "leave"

	// Tombstone is the member "tombstone" state
	Tombstone = "tombstone"
)

// A Member is a member in the member list
type Member struct {
	Address     string   `json:"address"`
	Status      string   `json:"status"`
	Incarnation int64    `json:"incarnationNumber"`
	Labels      LabelMap `json:"labels,omitempty"`
}

// LabelMap is a type Used by Member to store the labels of a member. It stores
// string to string mappings containing user data that is gossiped around in SWIM.
type LabelMap map[string]string

// suspect interface
func (m Member) address() string {
	return m.Address
}

func (m Member) incarnation() int64 {
	return m.Incarnation
}

func (m *Member) populateFromChange(c *Change) {
	m.Address = c.Address
	m.Incarnation = c.Incarnation
	m.Status = c.Status
	m.Labels = c.Labels
}

// checksumString fills a buffer that is passed with the contents that this node
// needs to add to the checksum string.
func (m Member) checksumString(b *bytes.Buffer) {
	fmt.Fprintf(b, "%s%s%v", m.Address, m.Status, m.Incarnation)
	m.Labels.checksumString(b)
}

// checksumString adds the label portion of the checksum to the buffer that is
// passed in. The string will not be appended in the case where labels are not
// set on this member. This is for backwards compatibility reasons with older
// versions.
func (l LabelMap) checksumString(b *bytes.Buffer) {
	checksum := l.checksum()

	if checksum == 0 {
		// we don't write the checksum of the labels if the value of the checksum
		// is 0 (zero) to be backwards compatible with ringpop applications on
		// an older version. This only works if the newer version does not use
		// labels
		return
	}

	// write #labels<checksum> to the buffer which will be appended to the
	// checksum string for the node.
	b.WriteString("#labels")
	b.WriteString(strconv.Itoa(int(checksum)))
}

// checksum computes a checksum for the labels. It will return 0 (zero) when no
// labels are set, but 0 does not indicate that no labels are set. It could be
// possible that 0 is computed as the checksum.
func (l LabelMap) checksum() int32 {
	var lb bytes.Buffer
	var checksum uint32
	for key, value := range l {
		// decide if we want to escape both fields to enforce uniqueness of the
		// key-value checksum.
		lb.WriteString("/")
		lb.WriteString(key)
		lb.WriteString("/")
		lb.WriteString(value)

		checksum = checksum ^ farm.Fingerprint32(lb.Bytes())

		lb.Reset()
	}

	var signedChecksum int32
	// This line converts an unsigned integer to a signed integer (32 bits).
	// It is needed to be able to calculate the same value in nodejs for
	// ringpop-node and the integration tests in ringpop-common.
	signedChecksum = int32(checksum>>1)<<1 | int32(checksum&uint32(1))
	return signedChecksum
}

// shuffles slice of members pseudo-randomly, returns new slice
func shuffle(members []*Member) []*Member {
	newMembers := make([]*Member, len(members), cap(members))
	newIndexes := rand.Perm(len(members))

	for o, n := range newIndexes {
		newMembers[n] = members[o]
	}

	return newMembers
}

// acceptGossip evaluates the rules of swim to accept the gossip as the new state
// of the member.
func acceptGossip(old *Member, gossip *Member) bool {
	// tombstones will not be accepted if we have no knowledge about the member
	if gossip.Status == Tombstone && old == nil {
		return false
	}

	// accept the gossip if we learn about the member through a gossip
	if old == nil {
		return true
	}

	// gossips with a higher incarnation number will always be accepted since
	// it is a newer version of the member than we know
	if gossip.Incarnation > old.Incarnation {
		return true
	}

	// gossips with a lower incarnation number will never be accepted as we
	// have a newer version of the member already
	if gossip.Incarnation < old.Incarnation {
		return false
	}

	// now we know that the incarnation number of the gossip and the current
	// view of the member are the same 'age'. Lets evaluate member state to see
	// which version to pick

	// if the status of the gossip takes precedence over the status of our
	// current member we will accept the gossip.
	if statePrecedence(gossip.Status) > statePrecedence(old.Status) {
		return true
	}

	// TODO add check to deterministically pick a member based on the labels

	// in the end there is no reason to accept the gossip, we already have the
	// latest view of the node.
	return false
}

func statePrecedence(s string) int {
	switch s {
	case Alive:
		return 0
	case Suspect:
		return 1
	case Faulty:
		return 2
	case Leave:
		return 3
	case Tombstone:
		return 4
	default:
		// unknown states will never have precedence
		return -1
	}
}

func (m *Member) isReachable() bool {
	return m.Status == Alive || m.Status == Suspect
}

// A Change is a change a member to be applied
type Change struct {
	Source            string            `json:"source"`
	SourceIncarnation int64             `json:"sourceIncarnationNumber"`
	Address           string            `json:"address"`
	Incarnation       int64             `json:"incarnationNumber"`
	Status            string            `json:"status"`
	Tombstone         bool              `json:"tombstone,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	// Use util.Timestamp for bi-direction binding to time encoded as
	// integer Unix timestamp in JSON
	Timestamp util.Timestamp `json:"timestamp"`
}

// validateIncoming validates incoming changes before they are passed into the
// swim state machine. This is usefull to make late adjustments to incoming
// changes to transform some legacy wire protocol changes into new swim terminology
func (c Change) validateIncoming() Change {
	if c.Status == Faulty && c.Tombstone {
		c.Status = Tombstone
	}
	return c
}

// validateOutgoing validates outgoing changes before they are passed to the module
// responsible for sending the change to the other side. This can be used to make sure
// that our changes are parsable by older version of ringpop-go to prevent unwanted
// behavior when incompatible changes are sent to older versions.
func (c Change) validateOutgoing() Change {
	if c.Status == Tombstone {
		c.Status = Faulty
		c.Tombstone = true
	}
	return c
}

func (c *Change) populateSubject(m *Member) {
	if m == nil {
		return
	}
	c.Address = m.Address
	c.Incarnation = m.Incarnation
	c.Status = m.Status
	c.Labels = m.Labels
}

func (c *Change) populateSource(m *Member) {
	if m == nil {
		return
	}
	c.Source = m.Address
	c.SourceIncarnation = m.Incarnation
}

// suspect interface
func (c Change) address() string {
	return c.Address
}

func (c Change) incarnation() int64 {
	return c.Incarnation
}

func (c Change) overrides(c2 Change) bool {
	if c.Incarnation > c2.Incarnation {
		return true
	}
	if c.Incarnation < c2.Incarnation {
		return false
	}

	return statePrecedence(c.Status) > statePrecedence(c2.Status)
}

func (c Change) isPingable() bool {
	return c.Status == Alive || c.Status == Suspect
}
