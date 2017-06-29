package org.apache.beam.sdk.transforms.reflect;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Used by {@link ByteBuddyDoFnInvokerFactory} to Dynamically generates
 * {@link OnTimerInvoker} instances for invoking a particular
 * {@link DoFn.TimerId} on a particular {@link DoFn}.
 */

@AutoValue
abstract class OnTimerMethodSpecifier {
    public abstract Class<? extends DoFn<?, ?>> fnClass();
    public abstract String timerId();
    public static OnTimerMethodSpecifier
    create(Class<? extends DoFn<?, ?>> fnClass, String timerId){
        return  new AutoValue_OnTimerMethodSpecifier(fnClass, timerId);
    }
}
