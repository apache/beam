package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;

public interface WindowProperties<LABEL> {

  WindowID<LABEL> getWindowID();

  long getEmissionWatermark();

}
