package kms

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"aws-in-a-box/arn"
	"aws-in-a-box/services/kms/types"
)

var kmsOptions = Options{
	ArnGenerator: arn.Generator{
		AwsAccountId: "12345",
		Region:       "us-east-1",
	}}

func newKMSWithKeyReturningARN() (*KMS, string, string) {
	k, err := New(kmsOptions)
	if err != nil {
		panic(err)
	}
	output, awserr := k.CreateKey(CreateKeyInput{})
	if awserr != nil {
		panic(awserr)
	}

	return k, output.KeyMetadata.KeyId, output.KeyMetadata.Arn
}

func newKMSWithKey() (*KMS, string) {
	k, keyId, _ := newKMSWithKeyReturningARN()
	return k, keyId
}

func TestEncryptionContext(t *testing.T) {
	k, keyId := newKMSWithKey()

	plaintext := []byte("The quick brown fox jumps over the lazy dog")
	context := map[string]string{"k1": "v1", "k2": "v2"}
	encryptOutput, err := k.Encrypt(EncryptInput{
		KeyId:             keyId,
		EncryptionContext: context,
		Plaintext:         plaintext,
	})
	if err != nil {
		t.Fatal(err)
	}

	ciphertext := encryptOutput.CiphertextBlob

	badContexts := []map[string]string{
		nil,
		{"k1": "v1"},
		{"k1": "v2"},
		{"k2": "v2"},
		{"k1": "v2", "k2": "v1"},
	}
	for _, context := range badContexts {
		_, err := k.Decrypt(DecryptInput{
			CiphertextBlob:    ciphertext,
			EncryptionContext: context,
		})
		if err == nil {
			t.Fatal("Expected error, bad context")
		}
	}

	goodContexts := []map[string]string{
		{"k1": "v1", "k2": "v2"},
		{"k2": "v2", "k1": "v1"},
	}
	for _, context := range goodContexts {
		decryptOutput, err := k.Decrypt(DecryptInput{
			CiphertextBlob:    ciphertext,
			EncryptionContext: context,
		})
		if err != nil {
			t.Fatal("Unexpected error, bad context")
		}
		if !bytes.Equal(plaintext, decryptOutput.Plaintext) {
			t.Fatalf("bad encryption result; got %v, want %v", decryptOutput.Plaintext, plaintext)
		}
	}
}

func TestGenerateDataKey(t *testing.T) {
	k, keyId := newKMSWithKey()

	generateOutput, err := k.GenerateDataKey(GenerateDataKeyInput{
		NumberOfBytes: 256,
		KeyId:         keyId,
	})
	if err != nil {
		t.Fatal(err)
	}

	decryptOutput, err := k.Decrypt(DecryptInput{
		CiphertextBlob: generateOutput.CiphertextBlob,
	})
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(generateOutput.Plaintext, generateOutput.Plaintext) {
		t.Fatalf("bad encryption result; got %v, want %v", decryptOutput.Plaintext, generateOutput.Plaintext)
	}
}

func TestAliasCreateDelete(t *testing.T) {
	k, keyId := newKMSWithKey()

	output, err := k.ListAliases(ListAliasesInput{})
	if err != nil {
		t.Fatal(err)
	}
	if len(output.Aliases) != 0 {
		t.Fatal(output.Aliases)
	}

	_, err = k.CreateAlias(CreateAliasInput{
		AliasName:   "alias/short",
		TargetKeyId: keyId,
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, aliasInput := range []ListAliasesInput{{}, {KeyId: keyId}} {
		output, err = k.ListAliases(aliasInput)
		if err != nil {
			t.Fatal(err)
		}
		if len(output.Aliases) != 1 {
			t.Fatal(output.Aliases)
		}
		alias := output.Aliases[0]
		if alias.AliasName != "alias/short" {
			t.Fatal(alias.AliasName)
		}
		if alias.TargetKeyId != keyId {
			t.Fatal(alias.TargetKeyId)
		}
		if alias.AliasArn != "arn:aws:kms:us-east-1:12345:alias/short" {
			t.Fatal(alias.AliasArn)
		}
	}

	output, err = k.ListAliases(ListAliasesInput{KeyId: "foo"})
	if err != nil {
		t.Fatal(err)
	}
	if len(output.Aliases) != 0 {
		t.Fatal(output.Aliases)
	}

	_, err = k.DeleteAlias(DeleteAliasInput{
		AliasName: "alias/short",
	})
	if err != nil {
		t.Fatal(err)
	}

	output, err = k.ListAliases(ListAliasesInput{})
	if err != nil {
		t.Fatal(err)
	}
	if len(output.Aliases) != 0 {
		t.Fatal(output.Aliases)
	}

	_, err = k.DeleteAlias(DeleteAliasInput{
		AliasName: "alias/short",
	})
	if err == nil {
		t.Fatal("Cannot delete missing alias")
	}
}

func TestAliasNaming(t *testing.T) {
	k, keyId := newKMSWithKey()

	_, err := k.CreateAlias(CreateAliasInput{
		AliasName:   "short",
		TargetKeyId: keyId,
	})
	if err == nil {
		t.Fatal("Illegal alias name")
	}

	_, err = k.CreateAlias(CreateAliasInput{
		AliasName:   "alias/short",
		TargetKeyId: keyId,
	})
	if err != nil {
		t.Fatal("Legal alias name")
	}

	_, err = k.CreateAlias(CreateAliasInput{
		AliasName:   "alias/aws",
		TargetKeyId: keyId,
	})
	if err != nil {
		t.Fatal("Legal alias name")
	}

	_, err = k.CreateAlias(CreateAliasInput{
		AliasName:   "alias/aws/short",
		TargetKeyId: keyId,
	})
	if err == nil {
		t.Fatal("Reserved alias name")
	}

	_, err = k.CreateAlias(CreateAliasInput{
		AliasName:   "alias/short" + strings.Repeat("long", 100),
		TargetKeyId: keyId,
	})
	if err == nil {
		t.Fatal("Long alias name")
	}

	_, err = k.CreateAlias(CreateAliasInput{
		AliasName:   "alias/short$",
		TargetKeyId: keyId,
	})
	if err == nil {
		t.Fatal("Bad character, illegal alias name")
	}
}

func TestEnableDisableKey(t *testing.T) {
	k, keyId := newKMSWithKey()

	plaintext := []byte("The quick brown fox jumps over the lazy dog")
	encryptOutput, err := k.Encrypt(EncryptInput{
		KeyId:     keyId,
		Plaintext: plaintext,
	})
	if err != nil {
		t.Fatal(err)
	}
	ciphertext := encryptOutput.CiphertextBlob

	_, err = k.DisableKey(DisableKeyInput{
		KeyId: keyId,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = k.Encrypt(EncryptInput{
		KeyId:     keyId,
		Plaintext: plaintext,
	})
	if err == nil {
		t.Fatal("Should not allow")
	}

	_, err = k.Decrypt(DecryptInput{
		KeyId:          keyId,
		CiphertextBlob: ciphertext,
	})
	if err == nil {
		t.Fatal("Should not allow")
	}

	_, err = k.GenerateDataKeyWithoutPlaintext(GenerateDataKeyWithoutPlaintextInput{
		KeyId:         keyId,
		NumberOfBytes: 256,
	})
	if err == nil {
		t.Fatal("Should not allow")
	}

	_, err = k.EnableKey(EnableKeyInput{
		KeyId: keyId,
	})
	if err != nil {
		t.Fatal(err)
	}

	decryptOutput, err := k.Decrypt(DecryptInput{
		KeyId:          keyId,
		CiphertextBlob: ciphertext,
	})
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(plaintext, decryptOutput.Plaintext) {
		t.Fatalf("bad encryption result; got %v, want %v", decryptOutput.Plaintext, plaintext)
	}
}

func TestEncryptDecrypt(t *testing.T) {
	k, keyId, keyArn := newKMSWithKeyReturningARN()

	aliasName := "alias/key"
	_, err := k.CreateAlias(CreateAliasInput{
		AliasName:   aliasName,
		TargetKeyId: keyId,
	})
	if err != nil {
		t.Fatal(err)
	}

	aliasesOutput, err := k.ListAliases(ListAliasesInput{})
	if err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}

	keyIds := []string{keyId, keyArn, aliasName, aliasesOutput.Aliases[0].AliasArn}
	for _, id := range keyIds {
		plaintext := []byte("The quick brown fox jumps over the lazy dog")
		context := map[string]string{"k1": "v1", "k2": "v2"}
		encryptOutput, err := k.Encrypt(EncryptInput{
			KeyId:             keyId,
			EncryptionContext: context,
			Plaintext:         plaintext,
		})
		if err != nil {
			t.Fatal(err)
		}

		ciphertext := encryptOutput.CiphertextBlob

		decryptOutput, err := k.Decrypt(DecryptInput{
			KeyId:             id,
			CiphertextBlob:    ciphertext,
			EncryptionContext: context,
		})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(plaintext, decryptOutput.Plaintext) {
			t.Fatalf("bad encryption result; got %v, want %v", decryptOutput.Plaintext, plaintext)
		}
	}
}

func TestRSAEncryptDecrypt(t *testing.T) {
	k, err := New(kmsOptions)
	if err != nil {
		t.Fatal(err)
	}

	output, awserr := k.CreateKey(CreateKeyInput{
		KeySpec:  "RSA_4096",
		KeyUsage: "ENCRYPT_DECRYPT",
	})
	if awserr != nil {
		t.Fatal(awserr)
	}

	keyId := output.KeyMetadata.KeyId

	algorithms := []types.EncryptionAlgorithm{types.RsaSha1, types.RsaSha256}
	for _, algorithm := range algorithms {
		plaintext := []byte("The quick brown fox") //jumps over the lazy dog")
		encryptOutput, err := k.Encrypt(EncryptInput{
			KeyId:               keyId,
			Plaintext:           plaintext,
			EncryptionAlgorithm: algorithm,
		})
		if err != nil {
			t.Fatal(err)
		}

		ciphertext := encryptOutput.CiphertextBlob

		decryptOutput, err := k.Decrypt(DecryptInput{
			KeyId:               keyId,
			CiphertextBlob:      ciphertext,
			EncryptionAlgorithm: algorithm,
		})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(plaintext, decryptOutput.Plaintext) {
			t.Fatalf("bad encryption result; got %v, want %v", decryptOutput.Plaintext, plaintext)
		}
	}
}

func TestInvalidCiphertext(t *testing.T) {
	k, keyId := newKMSWithKey()
	plaintext := []byte("The quick brown fox jumps over the lazy dog")

	context := map[string]string{"k1": "v1", "k2": "v2"}
	encryptOutput, err := k.Encrypt(EncryptInput{
		KeyId:             keyId,
		EncryptionContext: context,
		Plaintext:         plaintext,
	})
	if err != nil {
		t.Fatal(err)
	}

	ciphertext := encryptOutput.CiphertextBlob

	_, err = k.Decrypt(DecryptInput{
		KeyId:          keyId,
		CiphertextBlob: ciphertext,
		// No context
	})
	if !reflect.DeepEqual(err, InvalidCiphertextException("cipher: message authentication failed")) {
		t.Fatal("bad err", err)
	}

	_, err = k.Decrypt(DecryptInput{
		KeyId:             keyId,
		CiphertextBlob:    []byte("nope"),
		EncryptionContext: context,
	})
	if !reflect.DeepEqual(err, InvalidCiphertextException("")) {
		t.Fatal("bad err", err)
	}
}

func TestPersistedAliasesSurviveRestart(t *testing.T) {
	options := kmsOptions
	options.PersistDir = t.TempDir()

	k, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	createOutput, awserr := k.CreateKey(CreateKeyInput{})
	if awserr != nil {
		t.Fatal(awserr)
	}
	keyId := createOutput.KeyMetadata.KeyId

	aliasName := "alias/persisted"
	_, awserr = k.CreateAlias(CreateAliasInput{
		AliasName:   aliasName,
		TargetKeyId: keyId,
	})
	if awserr != nil {
		t.Fatal(awserr)
	}

	plaintext := []byte("The quick brown fox jumps over the lazy dog")
	encryptOutput, awserr := k.Encrypt(EncryptInput{
		KeyId:     aliasName,
		Plaintext: plaintext,
	})
	if awserr != nil {
		t.Fatal(awserr)
	}

	// Simulate a restart by building a fresh KMS from the same persist dir.
	restarted, err := New(options)
	if err != nil {
		t.Fatal(err)
	}

	describeOutput, awserr := restarted.DescribeKey(DescribeKeyInput{KeyId: aliasName})
	if awserr != nil {
		t.Fatal(awserr)
	}
	if describeOutput.KeyMetadata.KeyId != keyId {
		t.Fatalf("alias resolves to %v, want %v", describeOutput.KeyMetadata.KeyId, keyId)
	}

	decryptOutput, awserr := restarted.Decrypt(DecryptInput{
		CiphertextBlob: encryptOutput.CiphertextBlob,
	})
	if awserr != nil {
		t.Fatal(awserr)
	}
	if !bytes.Equal(plaintext, decryptOutput.Plaintext) {
		t.Fatalf("bad decryption result; got %v, want %v", decryptOutput.Plaintext, plaintext)
	}
}
