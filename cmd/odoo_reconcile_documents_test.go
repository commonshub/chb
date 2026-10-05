package cmd

import (
	"reflect"
	"testing"
)

func TestStatementLineDigitRefs(t *testing.T) {
	line := odooStatementLineForReconcile{
		PaymentRef: "007603556405",
		Narration:  `EUROPEAN DIRECT DEBIT 27-03 CREDITOR: PROXIMUS CREDITOR REF.: 6014464884 MANDATE REF.: P000679520 12345`,
	}
	got := statementLineDigitRefs(line)
	want := []string{"6014464884", "679520", "7603556405"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("refs = %v, want %v (short runs dropped, leading zeros stripped)", got, want)
	}
	if got := refDigits("+++076/0355/6405+++"); got != "7603556405" {
		t.Errorf("refDigits = %q (structured reference, leading zero stripped)", got)
	}
}

func TestVendorRefDomain(t *testing.T) {
	d := vendorRefDomain([]string{"111111", "222222"})
	ops, leaves := 0, 0
	for _, x := range d {
		if x == "|" {
			ops++
		} else {
			leaves++
		}
	}
	if leaves != 4 || ops != 3 || d[0] != "|" || d[2] != "|" {
		t.Fatalf("domain %v: want 3 leading | for 4 leaves", d)
	}
	if d := vendorRefDomain([]string{"111111"}); len(d) != 3 || d[0] != "|" {
		t.Fatalf("single ref: %v", d)
	}
}
