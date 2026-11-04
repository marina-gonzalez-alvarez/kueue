/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduler

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/features"
	utiltas "sigs.k8s.io/kueue/pkg/util/tas"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingnode "sigs.k8s.io/kueue/pkg/util/testingjobs/node"
)

func makeReplacementTestNode(cluster, rack, host string) *corev1.Node {
	node := testingnode.MakeNode(host).
		Label("rack", rack).
		Label(corev1.LabelHostname, host).
		StatusAllocatable(corev1.ResourceList{
			corev1.ResourceCPU:  resource.MustParse("4"),
			corev1.ResourcePods: resource.MustParse("10"),
		}).Ready().Obj()
	if cluster != "" {
		node.Labels[constants.MultiKueueClusterLabel] = cluster
	}
	return node
}

// Centralized assignments carry the full path, starting with the cluster, so
// failed-node replacement must look nodes up by their cluster-qualified key.
func TestCentralizedTASReplacementLookups(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.MultiKueueCentralizedTAS, true)
	levels := []string{constants.MultiKueueClusterLabel, "rack", corev1.LabelHostname}
	assignment := &utiltas.TopologyAssignment{
		Levels: levels,
		Domains: []utiltas.TopologyDomainAssignment{
			{Values: []string{"worker1", "rack-0", "host-0"}, Count: 1},
			{Values: []string{"worker2", "rack-0", "host-0"}, Count: 1},
		},
	}
	cases := map[string]struct {
		nodes          []*corev1.Node
		wantStale      bool
		wantStaleValue string
	}{
		"every assigned node present": {
			nodes: []*corev1.Node{
				makeReplacementTestNode("worker1", "rack-0", "host-0"),
				makeReplacementTestNode("worker2", "rack-0", "host-0"),
			},
		},
		"a same-named node missing on one worker": {
			nodes: []*corev1.Node{
				makeReplacementTestNode("worker1", "rack-0", "host-0"),
			},
			wantStale:      true,
			wantStaleValue: "host-0",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, log := utiltesting.ContextWithLog(t)
			snapshot := newTASFlavorSnapshot(log, flavorInformation{TopologyName: "topology"}, newTopologyTree(levels, tc.nodes, 0), newDefaultSimulatorSnapshot())
			stale, staleValue := snapshot.IsTopologyAssignmentStale(assignment)
			if stale != tc.wantStale || staleValue != tc.wantStaleValue {
				t.Errorf("IsTopologyAssignmentStale() = (%t, %q), want (%t, %q)", stale, staleValue, tc.wantStale, tc.wantStaleValue)
			}
		})
	}

	t.Run("required domain stays on the surviving worker", func(t *testing.T) {
		_, log := utiltesting.ContextWithLog(t)
		nodes := []*corev1.Node{
			makeReplacementTestNode("worker1", "rack-0", "host-0"),
			makeReplacementTestNode("worker1", "rack-0", "host-1"),
			makeReplacementTestNode("worker2", "rack-0", "host-0"),
		}
		snapshot := newTASFlavorSnapshot(log, flavorInformation{TopologyName: "topology"}, newTopologyTree(levels, nodes, 0), newDefaultSimulatorSnapshot())
		tr := &TASPodSetRequests{
			PodSet: utiltestingapi.MakePodSet("ps", 2).RequiredTopologyRequest("rack").Obj(),
			Count:  2,
		}
		surviving := &utiltas.TopologyAssignment{
			Levels:  levels,
			Domains: []utiltas.TopologyDomainAssignment{{Values: []string{"worker1", "rack-0", "host-0"}, Count: 1}},
		}
		want := utiltas.DomainID([]string{"worker1", "rack-0"})
		if got := snapshot.requiredReplacementDomain(tr, surviving); got != want {
			t.Errorf("requiredReplacementDomain() = %q, want %q", got, want)
		}
	})
}
