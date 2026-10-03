package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	nostrsource "github.com/CommonsHub/chb/providers/nostr"
	"github.com/nbd-wtf/go-nostr"
)

// ── 1. Odoo URIs in every tier ────────────────────────────────────────────

func TestOdooURIsInEveryTier(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")
	t.Setenv("DATA_DIR", dataDir)
	t.Setenv("APP_DATA_DIR", filepath.Join(tmp, "app"))
	t.Setenv("ODOO_URL", "https://citizen-spring-vzw.odoo.com")
	t.Setenv("ODOO_DATABASE", "citizen-spring-vzw")
	os.MkdirAll(filepath.Join(tmp, "app", "settings"), 0o755)
	os.WriteFile(filepath.Join(tmp, "app", "settings", "rooms.json"), []byte(`{"rooms":[{"slug":"ostrom","name":"Ostrom Room"}]}`), 0o644)

	seedBills(t, dataDir, "2026", "08", []OdooOutgoingInvoice{
		{ID: 1234, Number: "CHB-S/2026/08/0001", MoveType: "in_invoice", State: "posted", PaymentState: "not_paid",
			InvoiceDate: "2026-08-03", TotalAmount: 10, ResidualAmount: 10, Currency: "EUR",
			Partner: OdooInvoicePartner{ID: 7, Name: "Jane Roe"}},
	})
	seedInvoices(t, dataDir, "2026", "08", []OdooOutgoingInvoice{
		{ID: 5678, Number: "CHB/2026/00300", MoveType: "out_invoice", State: "posted", PaymentState: "paid",
			InvoiceDate: "2026-08-10", TotalAmount: 121, UntaxedAmount: 100, Currency: "EUR",
			Partner:   OdooInvoicePartner{ID: 8, Name: "Carol Poe", Email: "carol@example.com"},
			LineItems: []OdooInvoiceLineItem{{ID: 1, ProductName: "Ostrom Room", DisplayType: "product", SubtotalAmount: 100, TotalAmount: 121, AccountCode: "700100"}}},
	})
	if _, err := generateAccountingFiles(dataDir); err != nil {
		t.Fatal(err)
	}
	generateBills(dataDir, "")
	bill := "odoo:citizen-spring-vzw.odoo.com:citizen-spring-vzw:account.move:1234"
	inv := "odoo:citizen-spring-vzw.odoo.com:citizen-spring-vzw:account.move:5678"
	for _, a := range Audiences {
		for _, scope := range []struct{ year, month string }{{"2026", "08"}, {"2026", ""}} {
			var ex ExpensesFile
			readTier(t, dataDir, scope.year, scope.month, a, "expenses.json", &ex)
			if len(ex.Expenses) != 1 || ex.Expenses[0].URI != bill {
				t.Errorf("%s %s expenses uri = %+v", a, scope, ex.Expenses)
			}
			var cu CustomersFile
			readTier(t, dataDir, scope.year, scope.month, a, "customers.json", &cu)
			if len(cu.Customers) != 1 || len(cu.Customers[0].Invoices) != 1 || cu.Customers[0].Invoices[0] != inv || cu.Customers[0].InvoiceCount != 1 {
				t.Errorf("%s %s customers = %+v", a, scope, cu.Customers)
			}
			var bk BookingsFile
			readTier(t, dataDir, scope.year, scope.month, a, "bookings.json", &bk)
			if len(bk.Rentals) != 1 || bk.Rentals[0].URI != inv {
				t.Errorf("%s %s rentals = %+v", a, scope, bk.Rentals)
			}
		}
		var pb BillsFile
		raw, err := os.ReadFile(audiencePath(dataDir, "latest", "", a, pendingBillsFile))
		if err != nil || json.Unmarshal(raw, &pb) != nil || len(pb.Bills) != 1 || pb.Bills[0].URI != bill {
			t.Errorf("%s pending-bills = %s", a, raw)
		}
	}
	// The anonymous public customer row carries the URI but not the person.
	pub := readTier(t, dataDir, "2026", "08", AudiencePublic, "customers.json", nil)
	if strings.Contains(pub, "Carol") || !strings.Contains(pub, inv) {
		t.Errorf("public customers = %s", pub)
	}
	if _, err := enforceAudiencePolicy(AudiencePublic, "x.json", []byte(`{"u":"`+inv+`"}`)); err != nil {
		t.Errorf("an Odoo URI must pass the public policy: %v", err)
	}
	if _, err := enforceAudiencePolicy(AudienceMembers, "x.json", []byte(`{"c":"stripe:cus_ABCDEFGH1234"}`)); err == nil {
		t.Error("a Stripe customer id must be refused below stewards")
	}
}

// ── 2. Nostr: trust list, comments ────────────────────────────────────────

func signedEvent(t *testing.T, sk string, created int64, tags nostr.Tags, content string) NostrEvent {
	t.Helper()
	ev := nostr.Event{Kind: 1111, CreatedAt: nostr.Timestamp(created), Tags: tags, Content: content}
	if err := ev.Sign(sk); err != nil {
		t.Fatal(err)
	}
	out := NostrEvent{ID: ev.ID, PubKey: ev.PubKey, CreatedAt: int64(ev.CreatedAt), Kind: ev.Kind, Content: ev.Content, Sig: ev.Sig}
	for _, tg := range ev.Tags {
		out.Tags = append(out.Tags, []string(tg))
	}
	return out
}

func TestAnnotationTrustAndComments(t *testing.T) {
	trustedSK, strangerSK := nostr.GeneratePrivateKey(), nostr.GeneratePrivateKey()
	trustedPK, _ := nostr.GetPublicKey(trustedSK)
	trusted := map[string]bool{trustedPK: true}
	uri := "odoo:h:db:account.move:1"

	good := signedEvent(t, trustedSK, 100, nostr.Tags{{"i", uri}, {"k", "odoo:account.move"}, {"category", "drinks"}}, "fridge restock")
	stranger := signedEvent(t, strangerSK, 300, nostr.Tags{{"i", uri}, {"k", "odoo:account.move"}, {"category", "spam"}}, "spam")
	comment := signedEvent(t, trustedSK, 400, nostr.Tags{{"I", uri}, {"K", "odoo:account.move"}, {"i", uri}, {"k", "odoo:account.move"}}, "nice photo!")
	forged := stranger
	forged.PubKey = trustedPK // claims to be trusted, signature does not match

	got := annotationsFromEvents(map[string]NostrEvent{good.ID: good, stranger.ID: stranger, comment.ID: comment, "forged": forged}, trusted)
	a := got[uri]
	if a == nil || a.Category != "drinks" || a.Description != "fridge restock" {
		t.Fatalf("annotation = %+v: the untrusted, forged and comment events must all be ignored", a)
	}
	if acceptAnnotationEvent(comment, trusted) {
		t.Error("an event with an uppercase I is a comment, never an annotation")
	}
	newer := signedEvent(t, trustedSK, 200, nostr.Tags{{"i", uri}, {"category", "cold-drinks"}}, "")
	got = annotationsFromEvents(map[string]NostrEvent{good.ID: good, newer.ID: newer}, trusted)
	if got[uri].Category != "cold-drinks" {
		t.Error("the newest trusted snapshot wins")
	}
	if strings.Contains(strings.Join(annotationTags(uri)[0], ""), "I") || len(annotationTags(uri)) != 2 {
		t.Errorf("annotation tags = %v (lowercase i/k only)", annotationTags(uri))
	}
}

// A trusted annotation pulled into odoo-annotations.json reaches the
// expense in every tier via its uri; an untrusted cached entry does not.
func TestOdooAnnotationsAppliedByURI(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")
	t.Setenv("DATA_DIR", dataDir)
	t.Setenv("APP_DATA_DIR", filepath.Join(tmp, "app"))
	t.Setenv("ODOO_URL", "https://h.odoo.com")
	t.Setenv("ODOO_DATABASE", "h")
	trustedSK := nostr.GeneratePrivateKey()
	trustedPK, _ := nostr.GetPublicKey(trustedSK)
	writeFile(t, filepath.Join(tmp, "app", "settings", "settings.json"), `{"nostr":{"trustedAuthors":["`+trustedPK+`"]}}`)

	seedBills(t, dataDir, "2026", "08", []OdooOutgoingInvoice{
		{ID: 1, Number: "B1", MoveType: "in_invoice", State: "posted", PaymentState: "paid", InvoiceDate: "2026-08-03", TotalAmount: 10, Currency: "EUR",
			Partner: OdooInvoicePartner{ID: 9, Name: "DelivCo SRL"}},
		{ID: 2, Number: "B2", MoveType: "in_invoice", State: "posted", PaymentState: "paid", InvoiceDate: "2026-08-04", TotalAmount: 20, Currency: "EUR",
			Partner: OdooInvoicePartner{ID: 9, Name: "DelivCo SRL"}},
	})
	u1, u2 := odooDocURI("account.move", 1, ""), odooDocURI("account.move", 2, "")
	cache := NostrAnnotationCache{Annotations: map[string]*TxAnnotation{
		u1: {URI: u1, Category: "cold-drinks", Event: "potluck-08", Description: "fridge restock", Author: trustedPK, CreatedAt: 1},
		u2: {URI: u2, Category: "spam", Author: strings.Repeat("ab", 32), CreatedAt: 2},
	}}
	if err := nostrsource.WriteJSON(dataDir, "2026", "08", cache, nostrsource.OdooAnnotationsFile); err != nil {
		t.Fatal(err)
	}
	if _, err := generateAccountingFiles(dataDir); err != nil {
		t.Fatal(err)
	}
	var ex ExpensesFile
	readTier(t, dataDir, "2026", "08", AudiencePublic, "expenses.json", &ex)
	byURI := map[string]Expense{}
	for _, e := range ex.Expenses {
		byURI[e.URI] = e
	}
	if e := byURI[u1]; e.Category != "cold-drinks" || e.Event != "potluck-08" || e.Note != "fridge restock" {
		t.Errorf("annotated expense = %+v", e)
	}
	if byURI[u2].Category == "spam" {
		t.Error("an untrusted cached annotation must not apply")
	}
}

// ── 3. Photos from public channels ───────────────────────────────────────

func TestPublicPhotosOnlyFromPublicChannels(t *testing.T) {
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")
	t.Setenv("DATA_DIR", dataDir)
	t.Setenv("APP_DATA_DIR", filepath.Join(tmp, "app"))
	writeFile(t, filepath.Join(tmp, "app", "settings", "settings.json"),
		`{"discord":{"channels":{"general":"111","activities":{"potluck":"222"},"rooms":"333"},"publicChannels":["general","activities.potluck"]}}`)
	img := func(id, ch string) ImageEntry {
		return ImageEntry{ID: id, ChannelID: ch, URL: "https://cdn.discordapp.com/x?ex=1", Message: "msg " + id,
			FilePath: "2026/08/providers/discord/images/" + id + ".jpg"}
	}
	images := []ImageEntry{img("a1", "111"), img("a2", "222"), img("a3", "333")}
	for _, im := range images {
		writeFile(t, filepath.Join(dataDir, im.FilePath), "jpegbytes-"+im.ID)
	}
	// A copy left over from when #rooms was public must go.
	writeFile(t, filepath.Join(dataDir, "2026", "08", "public", "images", "a3.jpg"), "old")

	copied, removed := publishPublicImages(dataDir, images, publicPhotoChannelIDs(), false)
	if copied != 2 || removed != 1 {
		t.Fatalf("copied=%d removed=%d", copied, removed)
	}
	st, err := os.Stat(filepath.Join(dataDir, "2026", "08", "public", "images", "a1.jpg"))
	if err != nil || st.Mode().Perm() != 0o644 {
		t.Fatalf("public copy: %v %v", err, st)
	}
	if dst, _ := os.Stat(filepath.Join(dataDir, "2026", "08", "public", "images")); dst.Mode().Perm() != 0o755 {
		t.Errorf("public images dir mode = %o", dst.Mode().Perm())
	}

	f := ImagesFile{Year: "2026", Month: "08", Count: 3, Images: images}
	pub := imagesFileForAudience(f, AudiencePublic)
	if pub.Count != 2 {
		t.Fatalf("public images = %+v", pub.Images)
	}
	for _, im := range pub.Images {
		if im.ChannelID == "333" {
			t.Error("a photo from a non-public channel reached public/")
		}
		if im.FilePath != "2026/08/public/images/"+im.ID+".jpg" || im.Message != "" || im.URL == "" {
			t.Errorf("public image = %+v", im)
		}
	}
	mem := imagesFileForAudience(f, AudienceMembers)
	for _, im := range mem.Images {
		if strings.Contains(im.FilePath, "providers/") {
			t.Errorf("members filePath points into providers/: %s", im.FilePath)
		}
		if im.ChannelID == "333" && im.FilePath != "" {
			t.Errorf("non-public photo has a public filePath: %s", im.FilePath)
		}
	}
	if len(mem.Images) != 3 {
		t.Error("members still see every photo")
	}
	if imagesFileForAudience(f, AudienceStewards).Images[2].FilePath != images[2].FilePath {
		t.Error("stewards keep the providers path")
	}
}
