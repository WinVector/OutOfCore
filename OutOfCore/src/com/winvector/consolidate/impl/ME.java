package com.winvector.consolidate.impl;

import java.util.Map;

public final class ME<A,B> implements Map.Entry<A,B> {
	private final A k;
	private B v;
	
	public ME(final A k, final B v) {
		this.k = k;
		this.v = v;
	}

	@Override
	public A getKey() {
		return k;
	}

	@Override
	public B getValue() {
		return v;
	}

	@Override
	public B setValue(final B arg0) {
		final B ov = v;
		v = arg0;
		return ov;
	}
	
	@Override
	public String toString() {
		return "<" + k + "," + v + ">";
	}
}

