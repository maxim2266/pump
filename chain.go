// Code generated by gen-chains tool. DO NOT EDIT.

package pump

// Chain2 composes 2 pipeline stages into one.
func Chain2[A, B, C any](s1 S[A, B], s2 S[B, C]) S[A, C] {
	return func(src G[A], yield func(C) error) error {
		return s2(Bind(src, s1), yield)
	}
}

// Chain3 composes 3 pipeline stages into one.
func Chain3[A, B, C, D any](s1 S[A, B], s2 S[B, C], s3 S[C, D]) S[A, D] {
	return Chain2(Chain2(s1, s2), s3)
}

// Chain4 composes 4 pipeline stages into one.
func Chain4[A, B, C, D, E any](s1 S[A, B], s2 S[B, C], s3 S[C, D], s4 S[D, E]) S[A, E] {
	return Chain2(Chain3(s1, s2, s3), s4)
}

// Chain5 composes 5 pipeline stages into one.
func Chain5[A, B, C, D, E, F any](s1 S[A, B], s2 S[B, C], s3 S[C, D], s4 S[D, E], s5 S[E, F]) S[A, F] {
	return Chain2(Chain4(s1, s2, s3, s4), s5)
}

// Chain6 composes 6 pipeline stages into one.
func Chain6[A, B, C, D, E, F, G any](s1 S[A, B], s2 S[B, C], s3 S[C, D], s4 S[D, E], s5 S[E, F], s6 S[F, G]) S[A, G] {
	return Chain2(Chain5(s1, s2, s3, s4, s5), s6)
}

// Chain7 composes 7 pipeline stages into one.
func Chain7[A, B, C, D, E, F, G, H any](s1 S[A, B], s2 S[B, C], s3 S[C, D], s4 S[D, E], s5 S[E, F], s6 S[F, G], s7 S[G, H]) S[A, H] {
	return Chain2(Chain6(s1, s2, s3, s4, s5, s6), s7)
}

// Chain8 composes 8 pipeline stages into one.
func Chain8[A, B, C, D, E, F, G, H, I any](s1 S[A, B], s2 S[B, C], s3 S[C, D], s4 S[D, E], s5 S[E, F], s6 S[F, G], s7 S[G, H], s8 S[H, I]) S[A, I] {
	return Chain2(Chain7(s1, s2, s3, s4, s5, s6, s7), s8)
}

// Chain9 composes 9 pipeline stages into one.
func Chain9[A, B, C, D, E, F, G, H, I, J any](s1 S[A, B], s2 S[B, C], s3 S[C, D], s4 S[D, E], s5 S[E, F], s6 S[F, G], s7 S[G, H], s8 S[H, I], s9 S[I, J]) S[A, J] {
	return Chain2(Chain8(s1, s2, s3, s4, s5, s6, s7, s8), s9)
}

// Chain10 composes 10 pipeline stages into one.
func Chain10[A, B, C, D, E, F, G, H, I, J, K any](s1 S[A, B], s2 S[B, C], s3 S[C, D], s4 S[D, E], s5 S[E, F], s6 S[F, G], s7 S[G, H], s8 S[H, I], s9 S[I, J], s10 S[J, K]) S[A, K] {
	return Chain2(Chain9(s1, s2, s3, s4, s5, s6, s7, s8, s9), s10)
}

// Chain11 composes 11 pipeline stages into one.
func Chain11[A, B, C, D, E, F, G, H, I, J, K, L any](s1 S[A, B], s2 S[B, C], s3 S[C, D], s4 S[D, E], s5 S[E, F], s6 S[F, G], s7 S[G, H], s8 S[H, I], s9 S[I, J], s10 S[J, K], s11 S[K, L]) S[A, L] {
	return Chain2(Chain10(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10), s11)
}

// Chain12 composes 12 pipeline stages into one.
func Chain12[A, B, C, D, E, F, G, H, I, J, K, L, M any](s1 S[A, B], s2 S[B, C], s3 S[C, D], s4 S[D, E], s5 S[E, F], s6 S[F, G], s7 S[G, H], s8 S[H, I], s9 S[I, J], s10 S[J, K], s11 S[K, L], s12 S[L, M]) S[A, M] {
	return Chain2(Chain11(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11), s12)
}

// Chain13 composes 13 pipeline stages into one.
func Chain13[A, B, C, D, E, F, G, H, I, J, K, L, M, O any](s1 S[A, B], s2 S[B, C], s3 S[C, D], s4 S[D, E], s5 S[E, F], s6 S[F, G], s7 S[G, H], s8 S[H, I], s9 S[I, J], s10 S[J, K], s11 S[K, L], s12 S[L, M], s13 S[M, O]) S[A, O] {
	return Chain2(Chain12(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12), s13)
}

// Chain14 composes 14 pipeline stages into one.
func Chain14[A, B, C, D, E, F, G, H, I, J, K, L, M, O, P any](s1 S[A, B], s2 S[B, C], s3 S[C, D], s4 S[D, E], s5 S[E, F], s6 S[F, G], s7 S[G, H], s8 S[H, I], s9 S[I, J], s10 S[J, K], s11 S[K, L], s12 S[L, M], s13 S[M, O], s14 S[O, P]) S[A, P] {
	return Chain2(Chain13(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13), s14)
}

// Chain15 composes 15 pipeline stages into one.
func Chain15[A, B, C, D, E, F, G, H, I, J, K, L, M, O, P, Q any](s1 S[A, B], s2 S[B, C], s3 S[C, D], s4 S[D, E], s5 S[E, F], s6 S[F, G], s7 S[G, H], s8 S[H, I], s9 S[I, J], s10 S[J, K], s11 S[K, L], s12 S[L, M], s13 S[M, O], s14 S[O, P], s15 S[P, Q]) S[A, Q] {
	return Chain2(Chain14(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14), s15)
}

// Chain16 composes 16 pipeline stages into one.
func Chain16[A, B, C, D, E, F, G, H, I, J, K, L, M, O, P, Q, R any](s1 S[A, B], s2 S[B, C], s3 S[C, D], s4 S[D, E], s5 S[E, F], s6 S[F, G], s7 S[G, H], s8 S[H, I], s9 S[I, J], s10 S[J, K], s11 S[K, L], s12 S[L, M], s13 S[M, O], s14 S[O, P], s15 S[P, Q], s16 S[Q, R]) S[A, R] {
	return Chain2(Chain15(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15), s16)
}