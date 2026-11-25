// SPDX-License-Identifier: AGPL-3.0-only

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ZoneClaimState is the outcome of evaluating a claim set for one entry.
// +kubebuilder:validation:Enum=Active;Reserved;Contested
type ZoneClaimState string

const (
	ZoneClaimActive    ZoneClaimState = "Active"
	ZoneClaimReserved  ZoneClaimState = "Reserved"
	ZoneClaimContested ZoneClaimState = "Contested"
)

// ZoneClaimReason says why an entry is Contested.
// +kubebuilder:validation:Enum=ClaimedByAnotherProject;ParentClaimedByAnotherProject;ChildClaimedByAnotherProject;DuplicateZoneInProject
type ZoneClaimReason string

const (
	ZoneClaimClaimedByAnotherProject       ZoneClaimReason = "ClaimedByAnotherProject"
	ZoneClaimParentClaimedByAnotherProject ZoneClaimReason = "ParentClaimedByAnotherProject"
	ZoneClaimChildClaimedByAnotherProject  ZoneClaimReason = "ChildClaimedByAnotherProject"
	ZoneClaimDuplicateZoneInProject        ZoneClaimReason = "DuplicateZoneInProject"
)

// ZoneClaimProject identifies the project that owns a claim.
type ZoneClaimProject struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// +optional
	// +kubebuilder:validation:MaxLength=253
	Organization string `json:"organization,omitempty"`
}

// ZoneClaimZone identifies the DNSZone bound to a claim.
type ZoneClaimZone struct {
	// Cluster is the multicluster cluster name of the zone's project control
	// plane, without a leading slash.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[^/]`
	Cluster string `json:"cluster"`

	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Namespace string `json:"namespace"`

	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// UID is empty on an entry imported from the per-name accounting records
	// until its zone first binds it.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	UID string `json:"uid,omitempty"`
}

// ZoneClaimEntry is one project's claim on a name and every name below it.
// +kubebuilder:validation:XValidation:rule="has(self.project) != (has(self.operatorSeeded) && self.operatorSeeded)",message="project is set exactly when the entry is not operator-seeded"
// +kubebuilder:validation:XValidation:rule="!(has(self.operatorSeeded) && self.operatorSeeded) || self.state == 'Reserved'",message="an operator-seeded entry is always Reserved"
// +kubebuilder:validation:XValidation:rule="self.state != 'Active' || has(self.zone)",message="an Active entry has a zone"
// +kubebuilder:validation:XValidation:rule="self.state != 'Reserved' || !has(self.zone)",message="a Reserved entry has no zone"
// +kubebuilder:validation:XValidation:rule="has(self.reason) == (self.state == 'Contested')",message="reason is set exactly when the entry is Contested"
// +kubebuilder:validation:XValidation:rule="has(self.blockedBy) == (has(self.reason) && self.reason in ['ClaimedByAnotherProject', 'ParentClaimedByAnotherProject', 'ChildClaimedByAnotherProject'])",message="blockedBy is set exactly when another project's entry blocks this one"
type ZoneClaimEntry struct {
	// Name is the zone name the entry claims. It accepts every name
	// DNSZone.spec.domainName accepts, so any accounting record can be imported.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)+$`
	Name string `json:"name"`

	// Project is empty only on an operator-seeded entry, which blocks every
	// project.
	// +optional
	Project *ZoneClaimProject `json:"project,omitempty"`

	// Zone is the zone bound to the entry. An Active entry always has one, a
	// Reserved entry never does, and a Contested entry may have lost it.
	// +optional
	Zone *ZoneClaimZone `json:"zone,omitempty"`

	State ZoneClaimState `json:"state"`

	// +optional
	Reason ZoneClaimReason `json:"reason,omitempty"`

	// BlockedBy is the name of the other project's entry that blocks this one.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	BlockedBy string `json:"blockedBy,omitempty"`

	// PreexistingOverlap marks an entry imported from the per-name accounting
	// records that already overlapped another project's entry. Two such
	// entries never block each other.
	// +optional
	PreexistingOverlap bool `json:"preexistingOverlap,omitempty"`

	// OperatorSeeded marks an entry that no zone can bind, used to keep a name
	// out of reach.
	// +optional
	OperatorSeeded bool `json:"operatorSeeded,omitempty"`

	// Since is when the entry arrived or last changed state.
	Since metav1.Time `json:"since"`
}

// ZoneClaimSetSpec holds every claim in one registered domain.
type ZoneClaimSetSpec struct {
	// Entries are in arrival order. Order decides which claim is first.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=1024
	Entries []ZoneClaimEntry `json:"entries,omitempty"`
}

// ZoneClaimSet records which projects may serve names in one registered
// domain. It is named for the registered domain and installed only in the
// authoritative cluster.
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ZoneClaimSet struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ZoneClaimSetSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true
type ZoneClaimSetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ZoneClaimSet `json:"items"`
}

func init() {
	register(&ZoneClaimSet{}, &ZoneClaimSetList{})
}
