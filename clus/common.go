package main

func MaxInt(array ...int) int {
	const minInt = -int(^uint(0)>>1) - 1
	max := minInt
	for _, i := range array {
		if i > max {
			max = i
		}
	}
	return max
}
