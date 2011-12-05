package com.winvector.consolidate.impl;

import java.io.Serializable;

import com.winvector.consolidate.def.DataAdapter;

public final class IntegerAdapter implements DataAdapter<Integer>, Serializable {
	private static final long serialVersionUID = 1L;
	
	@Override
	public int compare(final Integer arg0, final Integer arg1) {
		return arg0.compareTo(arg1);
	}

	@Override
	public String toString(final Integer k) {
		return k.toString();
	}

	@Override
	public Integer parse(final String s) {
		return Integer.parseInt(s);
	}
}
