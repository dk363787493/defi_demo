package math

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWadMul(t *testing.T) {
	a := new(big.Int).Mul(big.NewInt(15), new(big.Int).Div(WAD, big.NewInt(10)))
	b := new(big.Int).Mul(big.NewInt(2), WAD)
	result := WadMul(a, b)
	expected := new(big.Int).Mul(big.NewInt(3), WAD)
	assert.Equal(t, expected.String(), result.String())
}

func TestWadDiv(t *testing.T) {
	a := new(big.Int).Mul(big.NewInt(3), WAD)
	b := new(big.Int).Mul(big.NewInt(2), WAD)
	result := WadDiv(a, b)
	expected := new(big.Int).Mul(big.NewInt(15), new(big.Int).Div(WAD, big.NewInt(10)))
	assert.Equal(t, expected.String(), result.String())
}

func TestRayMul(t *testing.T) {
	a := new(big.Int).Mul(big.NewInt(15), new(big.Int).Div(RAY, big.NewInt(10)))
	b := new(big.Int).Mul(big.NewInt(2), RAY)
	result := RayMul(a, b)
	expected := new(big.Int).Mul(big.NewInt(3), RAY)
	assert.Equal(t, expected.String(), result.String())
}

func TestRayDiv(t *testing.T) {
	a := new(big.Int).Mul(big.NewInt(3), RAY)
	b := new(big.Int).Mul(big.NewInt(2), RAY)
	result := RayDiv(a, b)
	expected := new(big.Int).Mul(big.NewInt(15), new(big.Int).Div(RAY, big.NewInt(10)))
	assert.Equal(t, expected.String(), result.String())
}

func TestRayDivZero(t *testing.T) {
	a := new(big.Int).Mul(big.NewInt(3), RAY)
	assert.Panics(t, func() { RayDiv(a, big.NewInt(0)) })
}

func TestWadToRay(t *testing.T) {
	oneWad := new(big.Int).Set(WAD)
	result := WadToRay(oneWad)
	expected := new(big.Int).Set(RAY)
	assert.Equal(t, expected.String(), result.String())
}

func TestRayToWad(t *testing.T) {
	oneRay := new(big.Int).Set(RAY)
	result := RayToWad(oneRay)
	expected := new(big.Int).Set(WAD)
	assert.Equal(t, expected.String(), result.String())
}

func TestCalculateLinearInterest(t *testing.T) {
	rate := new(big.Int).Mul(big.NewInt(5), new(big.Int).Exp(big.NewInt(10), big.NewInt(25), nil))
	timeDelta := uint64(365 * 24 * 3600)
	result := CalculateLinearInterest(rate, timeDelta)
	require.NotNil(t, result)
	expected := new(big.Int).Add(RAY, rate)
	assert.Equal(t, expected.String(), result.String())
}

func TestCalculateLinearInterestZeroDelta(t *testing.T) {
	rate := new(big.Int).Mul(big.NewInt(5), new(big.Int).Exp(big.NewInt(10), big.NewInt(25), nil))
	result := CalculateLinearInterest(rate, 0)
	assert.Equal(t, RAY.String(), result.String())
}
