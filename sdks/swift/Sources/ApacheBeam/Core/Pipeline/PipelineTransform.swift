
import Foundation

/// Enum for pipeline representable transforms as opposed to composite transforms
/// which are a user-side construct represented by PTransform
public enum PipelineTransform {
    case pardo(String,SerializableFn,[AnyPCollection])
    case impulse(AnyPCollection)
    case flatten([AnyPCollection],AnyPCollection)
    case groupByKey(AnyPCollection)
    case custom(String,Data,Environment?,[AnyPCollection])
    case composite(AnyPTransform)
}



