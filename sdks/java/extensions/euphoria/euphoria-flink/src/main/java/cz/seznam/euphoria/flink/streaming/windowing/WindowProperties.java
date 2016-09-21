package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;

public interface WindowProperties<GROUP, LABEL> {

  WindowID<GROUP, LABEL> getWindowID();

  long getEmissionWatermark();

}
