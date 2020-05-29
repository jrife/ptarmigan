package numbers

// Max returns the max integer
func Max(a, b int) int {
	if a < b {
		return b
	}

	return a
}

// Min returns the max integer
func Min(a, b int) int {
	if a > b {
		return b
	}

	return a
}
