package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;

public interface WindowProperties<WID extends Window> {

  WID getWindowID();

  long getEmissionWatermark();

}
