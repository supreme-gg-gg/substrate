// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package ateredis_test is an external (black-box) test package so it can
// depend on storetest, which itself depends on ateredis -- an internal test
// file in package ateredis can't do that without an import cycle.
package ateredis_test

import (
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
)

// TestContractSuite runs the backend-neutral store.Interface assertions
// against a miniredis-backed Persistence.
func TestContractSuite(t *testing.T) {
	storetest.RunContractTests(t, func(t *testing.T) store.Interface {
		s, cleanup := storetest.SetupTestStore(t)
		t.Cleanup(cleanup)
		return s
	})
}
