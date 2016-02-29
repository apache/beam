/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataartisans.flink.dataflow.translation.wrappers.streaming.state;

import java.io.IOException;

/**
 * The available types of state, as provided by the Beam SDK. This class is used for serialization/deserialization
 * purposes.
 * */
public enum StateType {

	VALUE(0),

	WATERMARK(1),

	LIST(2),

	ACCUMULATOR(3);

	private final int numVal;

	StateType(int value) {
		this.numVal = value;
	}

	public static void serialize(StateType type, StateCheckpointWriter output) throws IOException {
		if (output == null) {
			throw new IllegalArgumentException("Cannot write to a null output.");
		}

		if(type.numVal < 0 || type.numVal > 3) {
			throw new RuntimeException("Unknown State Type " + type + ".");
		}

		output.writeByte((byte) type.numVal);
	}

	public static StateType deserialize(StateCheckpointReader input) throws IOException {
		if (input == null) {
			throw new IllegalArgumentException("Cannot read from a null input.");
		}

		int typeInt = (int) input.getByte();
		if(typeInt < 0 || typeInt > 3) {
			throw new RuntimeException("Unknown State Type " + typeInt + ".");
		}

		StateType resultType = null;
		for(StateType st: values()) {
			if(st.numVal == typeInt) {
				resultType = st;
				break;
			}
		}
		return resultType;
	}
}
