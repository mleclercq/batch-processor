package com.foo;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class ConversionPipeline<T, E> {

    private final List<ConversionStep<T, E>> steps;
    private final Consumer<? super E>        errorSink;
    private final Consumer<? super T>        valueSink;
    private final BiConsumer<? super ConversionStep<T, E>, ? super Either<E, T>>
                                             stepPeeker;

    public ConversionPipeline(final List<ConversionStep<T, E>> steps,
                              final Consumer<? super E> errorSink,
                              final Consumer<? super T> valueSink,
                              final BiConsumer<? super ConversionStep<T, E>, ? super Either<E, T>> stepPeeker) {
        this.steps = steps;
        this.errorSink = errorSink;
        this.valueSink = valueSink;
        this.stepPeeker = stepPeeker;
    }

    public void process(final Stream<T> values) {
        Stream<Either<E, T>> processingStream =
                values.map(Either::right);
        for (final ConversionStep<T, E> step : steps) {
            processingStream = Either.StreamMonad
                    .flatMap(processingStream, step)
                    .peek(v -> stepPeeker.accept(step, v));
        }
        processingStream.forEach(v -> {
            if (v.isLeft()) {
                errorSink.accept(v.getLeft());
            } else {
                valueSink.accept(v.getRight());
            }
        });
    }

    protected void peekAfter(final ConversionStep<T, E> step,
                             final Either<E, T> value) {
        System.out.printf("After %s: %s\n", step.getName(), value);
    }
}
