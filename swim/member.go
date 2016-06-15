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
	"sort"
	"sync"

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
	sync.RWMutex
	Address     string            `json:"address"`
	Status      string            `json:"status"`
	Incarnation int64             `json:"incarnationNumber"`
	Labels      map[string]string `json:"labels,omitempty"`
}

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
func (m *Member) checksumString(b *bytes.Buffer) {
	fmt.Fprintf(b, "%s%s%v", m.Address, m.Status, m.Incarnation)

	// add the labels to the checksumstring
	labels := m.Labels
	if len(labels) > 0 {

		// to ensure deterministic string generation we will sort the keys
		// before adding them to the buffer
		keys := make([]string, 0, len(labels))
		for key, _ := range labels {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		for _, key := range keys {
			value := labels[key]
			// add the label seperator. By adding the seperator at the beginning
			// it will seperate the labels from the beginning for the checksum
			b.WriteString("-")
			b.WriteString(key)
			b.WriteString("-")
			b.WriteString(value)
		}
	}
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

// nonLocalOverride returns wether a change should be applied to the member and
// therefore overrides the state of the member. This will take into account the
// following rules in order:
//  1. the change must be about this node (same address)
//  2. the highest incarnation number indicates which state is newest
//  3. when incarnation numbers are the same the state will determine which
//     state shall be taken
//  4. TODO have deterministic label resolving when labels are not the same.
//     even though the labels should only be changed by the owning node bugs in
//     parts of the gossip protocol might not gossip or change labels
//     unintentionally. To make sure the owner learnes about it and give it a
//     chance to reincarnate the offending gossip should be deterministically
//     chosen and disseminated around.
func (m *Member) nonLocalOverride(change Change) bool {
	if change.Address != m.Address {
		return false
	}

	// change is younger than current member
	if change.Incarnation > m.Incarnation {
		return true
	}

	// change is older than current member
	if change.Incarnation < m.Incarnation {
		return false
	}

	// If the incarnation numbers are equal, we look at the state to
	// determine wether the change overrides this member.
	return statePrecedence(change.Status) > statePrecedence(m.Status)
}

// localOverride returns whether the change will override the state of the local
// member. When it will override the state the member should reincarnate itself
// to make sure that other members see this node in a correct state.
func (m *Member) localOverride(local string, change Change) bool {
	if m.Address != local {
		return false
	}

	// if the incarnation number of the change is smaller than the current
	// incarnation number it is not overriding the change
	if change.Incarnation < m.Incarnation {
		return false
	}

	return change.Status == Faulty || change.Status == Suspect || change.Status == Tombstone
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
