package math

import "math/big"

// WAD = 10^18, used for token amounts and percentages.
var WAD = new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)

// RAY = 10^27, used for interest rate index accumulation.
var RAY = new(big.Int).Exp(big.NewInt(10), big.NewInt(27), nil)

// HalfWAD = WAD / 2, for rounding.
var HalfWAD = new(big.Int).Div(WAD, big.NewInt(2))

// HalfRAY = RAY / 2, for rounding.
var HalfRAY = new(big.Int).Div(RAY, big.NewInt(2))

// SECONDS_PER_YEAR used by Aave-style interest calculation.
var SECONDS_PER_YEAR = big.NewInt(365 * 24 * 3600)

// WadMul multiplies two Wad values: (a * b + HalfWAD) / WAD.
func WadMul(a, b *big.Int) *big.Int {
	result := new(big.Int).Mul(a, b)
	result.Add(result, HalfWAD)
	result.Div(result, WAD)
	return result
}

// WadDiv divides two Wad values: (a * WAD + b/2) / b.
func WadDiv(a, b *big.Int) *big.Int {
	if b.Sign() == 0 {
		panic("wadray: division by zero")
	}
	halfB := new(big.Int).Div(b, big.NewInt(2))
	result := new(big.Int).Mul(a, WAD)
	result.Add(result, halfB)
	result.Div(result, b)
	return result
}

// RayMul multiplies two Ray values: (a * b + HalfRAY) / RAY.
func RayMul(a, b *big.Int) *big.Int {
	result := new(big.Int).Mul(a, b)
	result.Add(result, HalfRAY)
	result.Div(result, RAY)
	return result
}

// RayDiv divides two Ray values: (a * RAY + b/2) / b.
func RayDiv(a, b *big.Int) *big.Int {
	if b.Sign() == 0 {
		panic("wadray: division by zero")
	}
	halfB := new(big.Int).Div(b, big.NewInt(2))
	result := new(big.Int).Mul(a, RAY)
	result.Add(result, halfB)
	result.Div(result, b)
	return result
}

// WadToRay converts a Wad value to Ray by multiplying by 10^9.
func WadToRay(a *big.Int) *big.Int {
	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(9), nil)
	return new(big.Int).Mul(a, factor)
}

// RayToWad converts a Ray value to Wad by dividing by 10^9 (truncates).
func RayToWad(a *big.Int) *big.Int {
	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(9), nil)
	halfFactor := new(big.Int).Div(factor, big.NewInt(2))
	result := new(big.Int).Add(a, halfFactor)
	result.Div(result, factor)
	return result
}

// CalculateLinearInterest computes the linear interest multiplier.
// Returns RAY-scaled value: RAY + (rate * timeDelta / SECONDS_PER_YEAR).
func CalculateLinearInterest(rate *big.Int, timeDelta uint64) *big.Int {
	if timeDelta == 0 {
		return new(big.Int).Set(RAY)
	}
	result := new(big.Int).Mul(rate, new(big.Int).SetUint64(timeDelta))
	result.Div(result, SECONDS_PER_YEAR)
	result.Add(result, RAY)
	return result
}

// CalculateCompoundedInterest computes compounded interest using binomial approximation.
// Uses the first 3 terms of Taylor expansion.
func CalculateCompoundedInterest(rate *big.Int, timeDelta uint64) *big.Int {
	if timeDelta == 0 {
		return new(big.Int).Set(RAY)
	}

	td := new(big.Int).SetUint64(timeDelta)

	term1 := new(big.Int).Mul(rate, td)
	term1.Div(term1, SECONDS_PER_YEAR)

	rateSq := RayMul(rate, rate)
	tdMinus1 := new(big.Int).Sub(td, big.NewInt(1))
	term2 := new(big.Int).Mul(rateSq, td)
	term2.Mul(term2, tdMinus1)
	spySq := new(big.Int).Mul(SECONDS_PER_YEAR, SECONDS_PER_YEAR)
	term2.Div(term2, spySq)
	term2.Div(term2, big.NewInt(2))

	rateCubed := RayMul(rateSq, rate)
	tdMinus2 := new(big.Int).Sub(td, big.NewInt(2))
	term3 := new(big.Int).Mul(rateCubed, td)
	term3.Mul(term3, tdMinus1)
	term3.Mul(term3, tdMinus2)
	spyCubed := new(big.Int).Mul(spySq, SECONDS_PER_YEAR)
	term3.Div(term3, spyCubed)
	term3.Div(term3, big.NewInt(6))

	result := new(big.Int).Set(RAY)
	result.Add(result, term1)
	result.Add(result, term2)
	result.Add(result, term3)
	return result
}
