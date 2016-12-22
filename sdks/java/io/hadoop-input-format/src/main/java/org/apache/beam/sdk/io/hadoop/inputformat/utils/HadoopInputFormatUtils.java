package org.apache.beam.sdk.io.hadoop.inputformat.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class HadoopInputFormatUtils {
	private static final Set<Class> immutableTypes = new HashSet<Class>(
			Arrays.asList(String.class, Byte.class, Short.class, Integer.class,
					Long.class, Float.class, Double.class, Boolean.class,
					BigInteger.class, BigDecimal.class));

	public static boolean isImmutable(Object o) {
		return immutableTypes.contains(o.getClass());
	}
}
