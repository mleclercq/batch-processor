package com.foo;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import com.foo.Either.StreamMonad;
import org.junit.Assert;
import org.junit.Test;

public class TestEither {

    @Test
    public void testEitherFlatMapOnRight() {
        final List<String> list = Stream.of("toto", "titi").collect(
                toList());
        final Either<Object, List<String>> e = Either.right(list);
        final Either<Object, List<String>> mapped = e.flatMap(FLAT_MAP);
        System.out.println(mapped);
        Assert.assertTrue(mapped.isRight());
        Assert.assertEquals(list, mapped.getRight());
    }

    @Test
    public void testEitherFlatMapOnLeft() {
        final List<String> list = Collections.emptyList();
        final Either<Object, List<String>> e = Either.right(list);
        final Either<Object, List<String>> mapped = e.flatMap(FLAT_MAP);
        System.out.println(mapped);
        Assert.assertTrue(mapped.isLeft());
        Assert.assertEquals("empty", mapped.getLeft());
    }

    // Use this explicitly typed function to make sure Either.flatMap() method
    // is correctly declared to handle covariant operations.
    private static final Function<List<String>, Either<String, ArrayList<String>>>
            FLAT_MAP = l -> l.isEmpty() ?
                            Either.left("empty") :
                            Either.right(new ArrayList<>(l));

    @Test
    public void testStreamEitherFlatMap() {
        final Stream<Either<Object, String>> s =
                Stream.of("todo", "titi").map(Either::right);
        final Stream<Either<Object, String>> mapped =
                StreamMonad.flatMap(s, STREAM_FLAT_MAP);

        final List<Either<Object, String>> collected = mapped.collect(toList());
        System.out.println(collected);
        Assert.assertEquals(Stream
                                    .of(Either.right("t"),
                                        Either.right("d"),
                                        Either.<Object, String>left(4))
                                    .collect(toList()),
                            collected);
    }

    private static final Function<String, Stream<Either<Integer, String>>>
            STREAM_FLAT_MAP =
            s -> s.contains("o") ?
                 Stream.of(s.split("o")).map(Either::right) :
                 Stream.of(Either.left(s.length()));
}
