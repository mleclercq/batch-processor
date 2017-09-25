package com.foo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class BatchConversionPipeline<T, E>
        extends BatchIOStreamProcessor<T> {

    private final List<StepAndSink> steps;
    final         FileSync<T>       outSync;

    protected BatchConversionPipeline(
            final Stream<T> data,
            final int batchSize,
            final List<ConversionStep<T, E>> steps,
            final Path intermediateResultsDir,
            final Path outputFile) throws IOException {
        super(data, batchSize);
        this.steps = new ArrayList<>(steps.size());
        for (final ConversionStep<T, E> s : steps) {
            this.steps.add(new StepAndSink(s,
                    intermediateResultsDir != null ?
                    Either.consumer(new FileSync<>(
                            intermediateResultsDir.resolve(
                                    s.getName() + ".dropped"),
                            StandardCharsets.UTF_8), new FileSync<>(
                            intermediateResultsDir.resolve(
                                    s.getName() + ".out"),
                            StandardCharsets.UTF_8)) :
                    v -> {}));
        }
        this.outSync = new FileSync<>(outputFile, StandardCharsets.UTF_8);
    }

    @Override
    protected void process(final T[] batch) {
        Stream<Either<E, T>> processingStream =
                Stream.of(batch).map(Either::right);
        for (final StepAndSink step : steps) {
            processingStream = step.map(processingStream);
        }
        processingStream.forEach(v -> {
            if (v.isRight()) {
                outSync.accept(v.getRight());
            }
        });
    }

    private final class StepAndSink {
        private final ConversionStep<T, E>   step;
        private final Consumer<Either<E, T>> sink;

        private StepAndSink(
                final ConversionStep<T, E> step,
                final Consumer<Either<E, T>> sink) {
            this.step = step;
            this.sink = sink;
        }

        private Stream<Either<E, T>> map(final Stream<Either<E, T>> stream) {
            return Either.StreamMonad.flatMap(stream, step).peek(sink);
        }
    }
}
