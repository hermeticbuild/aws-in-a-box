package kms

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/uuid/v5"

	"aws-in-a-box/arn"
	"aws-in-a-box/atomicfile"
	"aws-in-a-box/awserrors"
	"aws-in-a-box/services/kms/key"
	"aws-in-a-box/services/kms/types"
)

type KeyId = string

type KMS struct {
	logger       *slog.Logger
	arnGenerator arn.Generator
	persistDir   string

	mu sync.Mutex

	// The keys here do not include the "alias/" prefix
	aliases map[string]KeyId
	keys    map[KeyId]*key.Key
}

type Options struct {
	Logger       *slog.Logger
	ArnGenerator arn.Generator
	PersistDir   string
}

const aliasesFilename = "aliases.json"

func New(options Options) (*KMS, error) {
	if options.Logger == nil {
		options.Logger = slog.Default()
	}

	keys := make(map[KeyId]*key.Key)
	aliases := make(map[string]KeyId)

	if options.PersistDir != "" {
		options.PersistDir = filepath.Join(options.PersistDir, "kms")
		err := os.MkdirAll(options.PersistDir, 0700)
		if err != nil {
			return nil, err
		}

		files, err := os.ReadDir(options.PersistDir)
		if err != nil {
			return nil, err
		}
		for _, file := range files {
			name := file.Name()
			fullPath := filepath.Join(options.PersistDir, name)
			if name == aliasesFilename {
				data, err := os.ReadFile(fullPath)
				if err != nil {
					return nil, err
				}
				err = json.Unmarshal(data, &aliases)
				if err != nil {
					return nil, err
				}
			} else if strings.HasSuffix(name, ".tmp") {
				os.Remove(fullPath)
			} else if strings.HasSuffix(name, ".json") {
				key, err := key.NewFromFile(fullPath)
				if err != nil {
					return nil, err
				}
				keys[key.Id()] = key
			}
		}
	}

	return &KMS{
		logger:       options.Logger,
		arnGenerator: options.ArnGenerator,
		persistDir:   options.PersistDir,
		aliases:      aliases,
		keys:         keys,
	}, nil
}

func (k *KMS) persistAliases() error {
	if k.persistDir == "" {
		return nil
	}

	data, err := json.Marshal(k.aliases)
	if err != nil {
		return err
	}
	aliasPath := filepath.Join(k.persistDir, aliasesFilename)
	_, err = atomicfile.Write(aliasPath, bytes.NewReader(data), 0600)
	return err
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_CreateKey.html
func (k *KMS) CreateKey(input CreateKeyInput) (*CreateKeyOutput, *awserrors.Error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	keyId := uuid.Must(uuid.NewV4()).String()

	for _, t := range input.Tags {
		if !isValidTagKey(t.TagKey) || !isValidTagValue(t.TagValue) {
			return nil, TagException("")
		}
	}

	tags := fromAPITags(input.Tags)

	keySpec := input.KeySpec
	if keySpec == "" {
		keySpec = input.CustomerMasterKeySpec
	}
	if keySpec == "" {
		keySpec = "SYMMETRIC_DEFAULT"
	}

	persistPath := ""
	if k.persistDir != "" {
		persistPath = filepath.Join(k.persistDir, keyId+".json")
	}

	usage := types.Usage(input.KeyUsage)

	options := key.Options{
		PersistPath: persistPath,

		CreationDate: time.Now(),
		Id:           keyId,
		KeySpec:      keySpec,
		Description:  input.Description,
		Tags:         tags,
		Usage:        usage,
	}

	var newKey *key.Key
	var err error

	switch keySpec {
	case "SYMMETRIC_DEFAULT":
		if options.Usage == "" {
			options.Usage = types.EncryptDecrypt
		}
		if options.Usage != types.EncryptDecrypt {
			return nil, ValidationException(fmt.Sprintf(
				"KeyUsage %s is not compatible with KeySpec %s", options.Usage, keySpec))
		}

		newKey, err = key.NewAES(options)
	case "HMAC_224", "HMAC_256", "HMAC_384", "HMAC_512":
		if usage != types.GenerateVerifyMAC {
			return nil, ValidationException(fmt.Sprintf(
				"KeyUsage %s is not compatible with KeySpec %s", options.Usage, keySpec))
		}
		bytes, _ := strconv.Atoi(keySpec[5:])
		newKey, err = key.NewHMAC(options, bytes)
	case "RSA_2048", "RSA_3072", "RSA_4096":
		if usage != types.EncryptDecrypt && usage != types.SignVerify {
			return nil, ValidationException(fmt.Sprintf(
				"KeyUsage %s is not compatible with KeySpec %s", options.Usage, keySpec))
		}

		bits, _ := strconv.Atoi(keySpec[4:])
		newKey, err = key.NewRSA(options, bits)
	case "ECC_NIST_P256", "ECC_NIST_P384", "ECC_NIST_P521":
		if usage != types.SignVerify {
			return nil, ValidationException(fmt.Sprintf(
				"KeyUsage %s is not compatible with KeySpec %s", options.Usage, keySpec))
		}
		newKey, err = key.NewECC(options, keySpec[10:])
	case "ECC_SECG_P256K1", "SM2":
		return nil, UnsupportedOperationException(
			fmt.Sprintf("KeySpec %s is not supported in this Region", keySpec))
	default:
		return nil, ValidationException("1 validation error detected: Value 'FAKE' at 'keySpec' failed to satisfy constraint: Member must satisfy enum value set: [RSA_2048, ECC_NIST_P384, ECC_NIST_P256, ECC_NIST_P521, HMAC_384, RSA_3072, ECC_SECG_P256K1, RSA_4096, SYMMETRIC_DEFAULT, HMAC_256, HMAC_224, HMAC_512]")
	}
	if err != nil {
		return nil, KMSInternalException(err.Error())
	}
	k.keys[keyId] = newKey

	return &CreateKeyOutput{
		KeyMetadata: k.toAPI(newKey),
	}, nil
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_DescribeKey.html
func (k *KMS) DescribeKey(input DescribeKeyInput) (*DescribeKeyOutput, *awserrors.Error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	key := k.lockedGetKey(input.KeyId)
	if key == nil {
		return nil, NotFoundException("")
	}

	return &DescribeKeyOutput{
		KeyMetadata: k.toAPI(key),
	}, nil
}

func (k *KMS) toAPI(key *key.Key) APIKeyMetadata {
	return APIKeyMetadata{
		Arn:                   k.arnGenerator.Generate("kms", "key", key.Id()),
		AWSAccountId:          k.arnGenerator.AwsAccountId,
		CustomerMasterKeySpec: key.KeySpec(),
		CreationDate:          key.CreationDate(),
		Description:           key.Description(),
		Enabled:               key.Enabled(),
		EncryptionAlgorithms:  key.EncryptionAlgorithms(),
		KeyId:                 key.Id(),
		KeySpec:               key.KeySpec(),
		KeyState:              key.KeyState(),
		KeyUsage:              key.Usage(),
		MacAlgorithms:         key.MacAlgorithms(),
		SigningAlgorithms:     key.SigningAlgorithms(),
		// TODO: built-in keys
		Origin: "AWS_KMS",
		// TODO: multiregion
		MultiRegion: false,
		// TODO: what is this
		KeyManager: "CUSTOMER",
	}
}

func (k *KMS) maybeIdAsArn(keyId string) string {
	if strings.HasPrefix(keyId, "arn:") {
		return keyId
	}
	return k.arnGenerator.Generate("kms", "key", keyId)
}

func (k *KMS) lockedGetKey(keyId string) *key.Key {
	// There are 4 possible ways to specify a key:
	// - Key ID: 1234abcd-12ab-34cd-56ef-1234567890ab
	// - Key ARN: arn:aws:kms:us-east-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab
	// - Alias name: alias/ExampleAlias
	// - Alias ARN: arn:aws:kms:us-east-2:111122223333:alias/ExampleAlias

	var isAlias bool
	if strings.HasPrefix(keyId, "arn:") {
		var resourceType string
		resourceType, keyId = arn.ExtractId(keyId)
		isAlias = resourceType == "alias"
	} else if strings.HasPrefix(keyId, "alias/") {
		_, keyId, isAlias = strings.Cut(keyId, "alias/")
	}

	if isAlias {
		var ok bool
		keyId, ok = k.aliases[keyId]
		if !ok {
			return nil
		}
	}

	return k.keys[keyId]
}

var aliasNameRe = regexp.MustCompile("^[a-zA-Z0-9/_-]+$")

// https://docs.aws.amazon.com/kms/latest/APIReference/API_CreateAlias.html
func (k *KMS) CreateAlias(input CreateAliasInput) (*CreateAliasOutput, *awserrors.Error) {
	var validationErrors []string

	if strings.HasPrefix(input.AliasName, "alias/aws/") {
		return nil, NotAuthorizedException("The alias name for a customer managed CMK cannot begin with alias/aws/.")
	}

	if len(input.AliasName) > 256 {
		validationErrors = append(validationErrors, fmt.Sprintf(
			"Value '%s' at 'aliasName' failed to satisfy constraint: Member must have length less than or equal to 256",
			input.AliasName,
		))
	}

	if !aliasNameRe.MatchString(input.AliasName) {
		validationErrors = append(validationErrors, fmt.Sprintf(
			"Value '%s' at 'aliasName' failed to satisfy constraint: Member must satisfy regular expression pattern: ^[a-zA-Z0-9:/_-]+$",
			input.AliasName,
		))
	}

	if len(validationErrors) > 0 {
		plural := ""
		if len(validationErrors) > 1 {
			plural = "s"
		}
		return nil, ValidationException(fmt.Sprintf(
			"%d validation error%s detected: %s",
			len(validationErrors), plural, strings.Join(validationErrors, "; ")),
		)
	}

	// Yeah these ValidationExceptions don't increase the count. Nice.
	if strings.HasPrefix(input.TargetKeyId, "alias/") {
		return nil, ValidationException("Aliases must refer to keys. Not aliases")
	}

	if !strings.HasPrefix(input.AliasName, "alias/") {
		return nil, ValidationException(
			`Alias must start with the prefix "alias/". Please see https://docs.aws.amazon.com/kms/latest/developerguide/kms-alias.html`)
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	_, err := uuid.FromString(input.TargetKeyId)
	if err != nil {
		return nil, NotFoundException("Invalid keyId " + input.TargetKeyId)
	}
	key := k.lockedGetKey(input.TargetKeyId)
	if key == nil {
		// TODO(zbarsky): this is a bit backwards, we should probably store keys based on their ARN
		return nil, NotFoundException(fmt.Sprintf(
			"Key '%s' does not exist", k.maybeIdAsArn(input.TargetKeyId)))
	}

	aliasName := strings.TrimPrefix(input.AliasName, "alias/")
	if _, ok := k.aliases[aliasName]; ok {
		return nil, AlreadyExistsException(fmt.Sprintf(
			"An alias with the name %s already exists",
			k.arnGenerator.Generate("kms", "alias", aliasName),
		))
	}

	k.aliases[aliasName] = key.Id()

	err = k.persistAliases()
	if err != nil {
		return nil, KMSInternalException(err.Error())
	}

	return nil, nil
}

func hasherForAlgorithm(algorithm types.SigningAlgorithm) (hash.Hash, *awserrors.Error) {
	switch algorithm {
	case types.RsaPssSHA256, types.RsaPkcs1SHA256, types.EcdsaSHA256:
		return sha256.New(), nil
	case types.RsaPssSHA384, types.RsaPkcs1SHA384, types.EcdsaSHA384:
		return sha512.New384(), nil
	case types.RsaPssSHA512, types.RsaPkcs1SHA512, types.EcdsaSHA512:
		return sha512.New(), nil
	default:
		return nil, UnsupportedOperationException("Unsupported SigningAlgorithm")
	}
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_Sign.html
func (k *KMS) Sign(input SignInput) (*SignOutput, *awserrors.Error) {
	if len(input.Message) > 4096 {
		return nil, ValidationException("Message too long; use digest")
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	signingKey := k.lockedGetKey(input.KeyId)
	if signingKey == nil {
		return nil, NotFoundException("")
	}

	if !signingKey.Enabled() {
		return nil, DisabledException("")
	}

	if signingKey.Usage() != types.SignVerify {
		return nil, UnsupportedOperationException("")
	}

	hasher, awserr := hasherForAlgorithm(input.SigningAlgorithm)
	if awserr != nil {
		return nil, awserr
	}

	var digest []byte
	switch input.MessageType {
	case "", "RAW":
		digest = hasher.Sum(input.Message)
	case "DIGEST":
		digest = input.Message
	default:
		return nil, ValidationException("Bad MessageType")
	}

	signature, err := signingKey.Sign(digest, input.SigningAlgorithm)
	if err != nil {
		if errors.Is(err, key.ErrBadAlgorithm) {
			return nil, UnsupportedOperationException(err.Error())
		}
		return nil, InvalidCiphertextException(err.Error())
	}

	return &SignOutput{
		KeyId:            signingKey.Id(),
		Signature:        signature,
		SigningAlgorithm: input.SigningAlgorithm,
	}, nil
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_Verify.html
func (k *KMS) Verify(input VerifyInput) (*VerifyOutput, *awserrors.Error) {
	if len(input.Message) > 4096 {
		return nil, ValidationException("Message too long; use digest")
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	signingKey := k.lockedGetKey(input.KeyId)
	if signingKey == nil {
		return nil, NotFoundException("")
	}

	if !signingKey.Enabled() {
		return nil, DisabledException("")
	}

	if signingKey.Usage() != types.SignVerify {
		return nil, UnsupportedOperationException("")
	}

	hasher, awserr := hasherForAlgorithm(input.SigningAlgorithm)
	if awserr != nil {
		return nil, awserr
	}

	var digest []byte
	switch input.MessageType {
	case "", "RAW":
		digest = hasher.Sum(input.Message)
	case "DIGEST":
		digest = input.Message
	default:
		return nil, ValidationException("Bad MessageType")
	}

	valid, err := signingKey.Verify(digest, input.Signature, input.SigningAlgorithm)
	if err != nil {
		if errors.Is(err, key.ErrBadAlgorithm) {
			return nil, UnsupportedOperationException(err.Error())
		}
		return nil, KMSInternalException("")
	}

	return &VerifyOutput{
		KeyId:            signingKey.Id(),
		SignatureValid:   valid,
		SigningAlgorithm: input.SigningAlgorithm,
	}, nil
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_UpdateAlias.html
func (k *KMS) UpdateAlias(input UpdateAliasInput) (*UpdateAliasOutput, *awserrors.Error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	// TODO: handle alias not starting with "alias/"
	aliasName := strings.TrimPrefix(input.AliasName, "alias/")
	currentKeyId, ok := k.aliases[aliasName]
	if !ok {
		return nil, NotFoundException("")
	}
	currentKey := k.keys[currentKeyId]

	targetKey := k.lockedGetKey(input.TargetKeyId)
	if targetKey == nil {
		return nil, NotFoundException("")
	}

	if currentKey.Usage() != targetKey.Usage() {
		return nil, UnsupportedOperationException("Usage must match")
	}

	if currentKey.IsAES() != targetKey.IsAES() ||
		currentKey.IsHMAC() != targetKey.IsHMAC() ||
		currentKey.IsAsymmetric() != targetKey.IsAsymmetric() {
		return nil, UnsupportedOperationException("Key type must match")
	}

	k.aliases[aliasName] = targetKey.Id()

	err := k.persistAliases()
	if err != nil {
		return nil, KMSInternalException(err.Error())
	}

	return nil, nil
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_DeleteAlias.html
func (k *KMS) DeleteAlias(input DeleteAliasInput) (*DeleteAliasOutput, *awserrors.Error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	// TODO: handle alias not starting with "alias/"
	aliasName := strings.TrimPrefix(input.AliasName, "alias/")
	if _, ok := k.aliases[aliasName]; !ok {
		return nil, NotFoundException("")
	}

	delete(k.aliases, aliasName)

	err := k.persistAliases()
	if err != nil {
		return nil, KMSInternalException(err.Error())
	}

	return nil, nil
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_DeleteAlias.html
func (k *KMS) ListAliases(input ListAliasesInput) (*ListAliasesOutput, *awserrors.Error) {
	output := &ListAliasesOutput{}

	k.mu.Lock()
	defer k.mu.Unlock()

	for alias, target := range k.aliases {
		if input.KeyId == "" || input.KeyId == target {
			output.Aliases = append(output.Aliases, APIAliasListEntry{
				AliasName:   "alias/" + alias,
				AliasArn:    k.arnGenerator.Generate("kms", "alias", alias),
				TargetKeyId: target,
			})
		}
	}

	return output, nil
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_GenerateDataKey.html
func (k *KMS) GenerateDataKey(input GenerateDataKeyInput) (*GenerateDataKeyOutput, *awserrors.Error) {
	numberOfBytes := input.NumberOfBytes
	if numberOfBytes < 0 || numberOfBytes > 1024 {
		return nil, XXXTodoException("Invalid number of bytes value")
	}
	if numberOfBytes == 0 {
		switch input.KeySpec {
		case "AES_256":
			numberOfBytes = 32
		case "AES_128":
			numberOfBytes = 16
		case "":
			return nil, InvalidParameterCombination("Must specify either KeySpec or NumberOfBytes")
		default:
			return nil, XXXTodoException("Invalid value for KeySpec")
		}
	}

	dataKey := make([]byte, numberOfBytes)
	rand.Read(dataKey)

	k.mu.Lock()
	defer k.mu.Unlock()
	key := k.lockedGetKey(input.KeyId)
	if key == nil {
		return nil, NotFoundException("")
	}

	if !key.Enabled() {
		return nil, DisabledException("")
	}

	if !key.IsAES() {
		return nil, UnsupportedOperationException("")
	}

	encryptedDataKey, err := key.Encrypt(dataKey, types.SymmetricDefault, input.EncryptionContext)
	if err != nil {
		return nil, KMSInternalException(err.Error())
	}

	return &GenerateDataKeyOutput{
		KeyId:          key.Id(),
		Plaintext:      dataKey,
		CiphertextBlob: encryptedDataKey,
	}, nil
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_GenerateDataKey.html
func (k *KMS) GenerateDataKeyWithoutPlaintext(
	input GenerateDataKeyWithoutPlaintextInput,
) (*GenerateDataKeyWithoutPlaintextOutput, *awserrors.Error) {
	output, err := k.GenerateDataKey(input)
	if err != nil {
		return nil, err
	}

	return &GenerateDataKeyWithoutPlaintextOutput{
		CiphertextBlob: output.CiphertextBlob,
		KeyId:          output.KeyId,
	}, nil
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_GenerateDataKeyPair.html
func (k *KMS) GenerateDataKeyPair(input GenerateDataKeyPairInput) (*GenerateDataKeyPairOutput, *awserrors.Error) {
	var pkey interface {
		Public() crypto.PublicKey
	}
	var err error

	switch input.KeyPairSpec {
	case "RSA_2048", "RSA_3072", "RSA_4096":
		bits, _ := strconv.Atoi(input.KeyPairSpec[4:])
		pkey, err = rsa.GenerateKey(rand.Reader, bits)
	case "ECC_NIST_P256":
		pkey, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	case "ECC_NIST_P384":
		pkey, err = ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	case "ECC_NIST_P521":
		pkey, err = ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	case "":
		return nil, ValidationException("Must specify KeyPairSpec")
	case "ECC_SECG_P256K1":
		// fallthrough
	default:
		return nil, ValidationException("Unknown value for KeyPair Spec")
	}
	if err != nil {
		return nil, KMSInternalException(err.Error())
	}

	serializedPublicKey, err := x509.MarshalPKIXPublicKey(pkey.Public())
	if err != nil {
		return nil, KMSInternalException(err.Error())
	}

	serializedPrivateKey, err := x509.MarshalPKCS8PrivateKey(pkey)
	if err != nil {
		return nil, KMSInternalException(err.Error())
	}

	k.mu.Lock()
	defer k.mu.Unlock()
	key := k.lockedGetKey(input.KeyId)
	if key == nil {
		return nil, NotFoundException("")
	}

	if !key.Enabled() {
		return nil, DisabledException("")
	}

	if key.Usage() != types.EncryptDecrypt || !key.IsAES() {
		return nil, UnsupportedOperationException("")
	}

	encryptedPrivateKey, err := key.Encrypt(serializedPrivateKey, types.SymmetricDefault, input.EncryptionContext)
	if err != nil {
		return nil, KMSInternalException(err.Error())
	}

	return &GenerateDataKeyPairOutput{
		KeyId:                    key.Id(),
		KeyPairSpec:              input.KeyPairSpec,
		PrivateKeyCiphertextBlob: encryptedPrivateKey,
		PrivateKeyPlaintext:      serializedPrivateKey,
		PublicKey:                serializedPublicKey,
	}, nil
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_GenerateDataKeyPairWithoutPlaintext.html
func (k *KMS) GenerateDataKeyPairWithoutPlaintext(
	input GenerateDataKeyPairWithoutPlaintextInput,
) (*GenerateDataKeyPairWithoutPlaintextOutput, *awserrors.Error) {
	output, err := k.GenerateDataKeyPair(input)
	if err != nil {
		return nil, err
	}
	return &GenerateDataKeyPairWithoutPlaintextOutput{
		KeyId:                    output.KeyId,
		KeyPairSpec:              output.KeyPairSpec,
		PrivateKeyCiphertextBlob: output.PrivateKeyCiphertextBlob,
		PublicKey:                output.PublicKey,
	}, nil
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_GenerateRandom.html
func (k *KMS) GenerateRandom(input GenerateRandomInput) (*GenerateRandomOutput, *awserrors.Error) {
	if input.NumberOfBytes < 0 || input.NumberOfBytes > 1024 {
		return nil, ValidationException("Invalid NumberOfBytes")
	}

	data := make([]byte, input.NumberOfBytes)
	rand.Read(data)

	return &GenerateRandomOutput{
		Plaintext: data,
	}, nil
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_Encrypt.html
func (k *KMS) Encrypt(input EncryptInput) (*EncryptOutput, *awserrors.Error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if len(input.Plaintext) == 0 || len(input.Plaintext) > 4096 {
		return nil, XXXTodoException("bad length")
	}

	if input.EncryptionAlgorithm == "" {
		input.EncryptionAlgorithm = types.SymmetricDefault
	}

	encryptionKey := k.lockedGetKey(input.KeyId)
	if encryptionKey == nil {
		return nil, NotFoundException("")
	}

	if !encryptionKey.Enabled() {
		return nil, DisabledException("")
	}

	if encryptionKey.Usage() != types.EncryptDecrypt {
		return nil, UnsupportedOperationException("")
	}

	ciphertext, err := encryptionKey.Encrypt(input.Plaintext,
		input.EncryptionAlgorithm, input.EncryptionContext)
	if err != nil {
		if errors.Is(err, key.ErrBadAlgorithm) {
			return nil, UnsupportedOperationException(err.Error())
		}
		return nil, KMSInternalException(err.Error())
	}

	return &EncryptOutput{
		CiphertextBlob:      ciphertext,
		EncryptionAlgorithm: input.EncryptionAlgorithm,
		KeyId:               encryptionKey.Id(),
	}, nil
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_GenerateMac.html
func (k *KMS) GenerateMac(input GenerateMacInput) (*GenerateMacOutput, *awserrors.Error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if len(input.Message) == 0 || len(input.Message) > 4096 {
		return nil, ValidationException("bad length")
	}

	macKey := k.lockedGetKey(input.KeyId)
	if macKey == nil {
		return nil, NotFoundException("")
	}

	if !macKey.Enabled() {
		return nil, DisabledException("")
	}

	if macKey.Usage() != types.GenerateVerifyMAC {
		return nil, UnsupportedOperationException("")
	}

	mac, err := macKey.GenerateMac(input.MacAlgorithm, input.Message)
	if err != nil {
		if errors.Is(err, key.ErrBadAlgorithm) {
			return nil, UnsupportedOperationException(err.Error())
		}
		return nil, KMSInternalException(err.Error())
	}

	return &GenerateMacOutput{
		KeyId:        macKey.Id(),
		MacAlgorithm: input.MacAlgorithm,
		Mac:          mac,
	}, nil
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_VerifyMac.html
func (k *KMS) VerifyMac(input VerifyMacInput) (*VerifyMacOutput, *awserrors.Error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if len(input.Message) == 0 || len(input.Message) > 4096 {
		return nil, ValidationException("bad length")
	}

	macKey := k.lockedGetKey(input.KeyId)
	if macKey == nil {
		return nil, NotFoundException("")
	}

	if !macKey.Enabled() {
		return nil, DisabledException("")
	}

	if macKey.Usage() != types.GenerateVerifyMAC {
		return nil, UnsupportedOperationException("")
	}

	mac, err := macKey.GenerateMac(input.MacAlgorithm, input.Message)
	if err != nil {
		if errors.Is(err, key.ErrBadAlgorithm) {
			return nil, UnsupportedOperationException(err.Error())
		}
		return nil, KMSInternalException(err.Error())
	}

	return &VerifyMacOutput{
		KeyId:        macKey.Id(),
		MacAlgorithm: input.MacAlgorithm,
		MacValid:     hmac.Equal(mac, input.Mac),
	}, nil
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_Decrypt.html
func (k *KMS) Decrypt(input DecryptInput) (*DecryptOutput, *awserrors.Error) {
	if input.EncryptionAlgorithm == "" {
		input.EncryptionAlgorithm = "SYMMETRIC_DEFAULT"
	}

	keyId := input.KeyId

	ciphertext := input.CiphertextBlob
	if len(ciphertext) == 0 {
		return nil, InvalidCiphertextException("")
	}

	if keyId == "" {
		// Passing KeyId is optional for symmetric encyption.
		// Run the opposite of Key.Encrypt to unpack the key that was used
		data := ciphertext
		keyArnLen, data := uint8(data[0]), data[1:]
		if len(data) < 4+int(keyArnLen) {
			return nil, InvalidCiphertextException("")
		}
		keyId = string(data[:keyArnLen])
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	encryptionKey := k.lockedGetKey(keyId)

	if encryptionKey == nil {
		return nil, NotFoundException("")
	}

	if !encryptionKey.Enabled() {
		return nil, DisabledException("")
	}

	if encryptionKey.IsAES() {
		if input.KeyId != "" && input.KeyId != encryptionKey.Id() {
			// TODO(zbarsky): we should flag this
			// return nil, IncorrectKeyException("")
			k.logger.Info("WRONG aes key", "expected", input.KeyId, "actual", encryptionKey.Id())
		}
		// AES can pack keyId into the ciphertext
		// This logic is the opposite of Key.Encrypt
		keyArnLen := uint8(ciphertext[0])
		if len(ciphertext) < 5+int(keyArnLen) {
			return nil, InvalidCiphertextException("")
		}
		ciphertext = ciphertext[keyArnLen+1:]
	}

	plaintext, err := encryptionKey.Decrypt(ciphertext, input.EncryptionAlgorithm, input.EncryptionContext)
	if err != nil {
		if errors.Is(err, key.ErrBadAlgorithm) {
			return nil, UnsupportedOperationException(err.Error())
		}
		return nil, InvalidCiphertextException(err.Error())
	}

	return &DecryptOutput{
		Plaintext:           plaintext,
		EncryptionAlgorithm: input.EncryptionAlgorithm,
		KeyId:               encryptionKey.Id(),
	}, nil
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_UpdateKeyDescription.html
func (k *KMS) UpdateKeyDescription(input UpdateKeyDescriptionInput) (*UpdateKeyDescriptionOutput, *awserrors.Error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	key := k.lockedGetKey(input.KeyId)
	if key == nil {
		return nil, NotFoundException("")
	}

	err := key.SetDescription(input.Description)
	if err != nil {
		return nil, KMSInternalException(err.Error())
	}
	return nil, nil
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_DisableKey.html
func (k *KMS) DisableKey(input DisableKeyInput) (*DisableKeyOutput, *awserrors.Error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	key := k.lockedGetKey(input.KeyId)
	if key == nil {
		return nil, NotFoundException("")
	}

	err := key.SetEnabled(false)
	if err != nil {
		return nil, KMSInternalException(err.Error())
	}
	return nil, nil
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_EnableKey.html
func (k *KMS) EnableKey(input EnableKeyInput) (*EnableKeyOutput, *awserrors.Error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	key := k.lockedGetKey(input.KeyId)
	if key == nil {
		return nil, NotFoundException("")
	}

	err := key.SetEnabled(true)
	if err != nil {
		return nil, KMSInternalException(err.Error())
	}
	return nil, nil
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_TagResource.html
func (k *KMS) TagResource(input TagResourceInput) (*TagResourceOutput, *awserrors.Error) {
	if strings.HasPrefix(input.KeyId, "alias/") {
		return nil, XXXTodoException("Cannot tag alias")
	}

	for _, t := range input.Tags {
		if !isValidTagKey(t.TagKey) || !isValidTagValue(t.TagValue) {
			return nil, TagException("")
		}
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	key := k.lockedGetKey(input.KeyId)
	if key == nil {
		return nil, NotFoundException("")
	}

	err := key.SetTags(fromAPITags(input.Tags))
	if err != nil {
		return nil, KMSInternalException(err.Error())
	}
	return nil, nil
}

func fromAPITags(apiTags []APITag) map[string]string {
	tags := make(map[string]string, len(apiTags))
	for _, t := range apiTags {
		tags[t.TagKey] = t.TagValue
	}
	return tags
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_UntagResource.html
func (k *KMS) UntagResource(input UntagResourceInput) (*UntagResourceOutput, *awserrors.Error) {
	if strings.HasPrefix(input.KeyId, "alias/") {
		return nil, XXXTodoException("Cannot tag alias")
	}

	for _, tagKey := range input.Tags {
		if !isValidTagKey(tagKey) {
			return nil, TagException("")
		}
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	key := k.lockedGetKey(input.KeyId)
	if key == nil {
		return nil, NotFoundException("")
	}

	err := key.DeleteTags(input.Tags)
	if err != nil {
		return nil, KMSInternalException(err.Error())
	}
	return nil, nil
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_ListResourceTags.html
func (k *KMS) ListResourceTags(input ListResourceTagsInput) (*ListResourceTagsOutput, *awserrors.Error) {
	if strings.HasPrefix(input.KeyId, "alias/") {
		return nil, XXXTodoException("Cannot tag alias")
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	key := k.lockedGetKey(input.KeyId)
	if key == nil {
		return nil, NotFoundException("")
	}

	output := &ListResourceTagsOutput{}
	for tagKey, tagValue := range key.Tags() {
		output.Tags = append(output.Tags, APITag{
			TagKey:   tagKey,
			TagValue: tagValue,
		})
	}

	return output, nil
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_ListKeys.html
func (k *KMS) ListKeys(input ListKeysInput) (*ListKeysOutput, *awserrors.Error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	output := &ListKeysOutput{}
	for _, key := range k.keys {
		output.Keys = append(output.Keys, APIKey{
			KeyId:  key.Id(),
			KeyArn: k.arnGenerator.Generate("kms", "key", key.Id()),
		})
	}

	return output, nil
}

// https://docs.aws.amazon.com/kms/latest/APIReference/API_ReEncrypt.html
func (k *KMS) ReEncrypt(input ReEncryptInput) (*ReEncryptOutput, *awserrors.Error) {
	decryptOutput, err := k.Decrypt(DecryptInput{
		CiphertextBlob:      input.CiphertextBlob,
		EncryptionAlgorithm: input.SourceEncryptionAlgorithm,
		EncryptionContext:   input.SourceEncryptionContext,
		KeyId:               input.SourceKeyId,
	})
	if err != nil {
		return nil, err
	}
	encryptOutput, err := k.Encrypt(EncryptInput{
		EncryptionAlgorithm: input.DestinationEncryptionAlgorithm,
		EncryptionContext:   input.DestinationEncryptionContext,
		KeyId:               input.DestinationKeyId,
		Plaintext:           decryptOutput.Plaintext,
	})
	if err != nil {
		return nil, err
	}

	return &ReEncryptOutput{
		CiphertextBlob:                 encryptOutput.CiphertextBlob,
		DestinationEncryptionAlgorithm: encryptOutput.EncryptionAlgorithm,
		KeyId:                          encryptOutput.KeyId,
		SourceEncryptionAlgorithm:      decryptOutput.EncryptionAlgorithm,
		SourceKeyId:                    decryptOutput.KeyId,
	}, nil
}

func isValidTagKey(tagKey string) bool {
	if strings.HasPrefix(tagKey, "aws:") {
		return false
	}

	if len(tagKey) == 0 || len(tagKey) > 128 {
		return false
	}

	return true
}

func isValidTagValue(tagValue string) bool {
	if len(tagValue) == 0 || len(tagValue) > 256 {
		return false
	}

	return true
}
