package com.example;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class Main {

    private static final ByteBufAllocator BUFFER_FACTORY = new PooledByteBufAllocator(true);
    private static final Random RANDOM = new Random();
   
    /*
    
    The `JoinSplit` flat-mapper below takes the input buffers and splits the
    input into equally sizes buffers.


    byte offset (10s):   0000000000111111111122222222223333333333444444444455555555556666666666
    byte offset (units): 0123456789012345678901234567890123456789012345678901234567890123456789
    Input Buffers:       <--><--------><----------------><---><-----------><------------><---->
    Output Buffers:      |--------||--------||--------||--------||--------||--------||--------|

    How do I make the reference counting correct? everything should have a
    count of zero at the end.

    */

    public static void main(String[] args) {
        List<ByteBuf> inputBuffers = new ArrayList<>();

        inputBuffers.add(randomBuffer(4));  //  0 +  4 -> no operations; not enough bytes.
        inputBuffers.add(randomBuffer(10)); //  4 + 10 -> will chop off the first 10 bytes, leaving 4.
        inputBuffers.add(randomBuffer(18)); //  4 + 18 -> will chop off 2x 10 bytes, leaving 2.
        inputBuffers.add(randomBuffer(5));  //  2 +  5 -> not enough bytes
        inputBuffers.add(randomBuffer(13)); //  7 + 13 -> will split into 2x 10 bytes
        inputBuffers.add(randomBuffer(14)); //  0 + 14 -> will chop off 10 bytes, leaving 4
        inputBuffers.add(randomBuffer(6));  //  4 +  6 -> last buffer, exactly 10 bytes

        List<ByteBuf> outputBuffers = inputBuffers.stream()
                .flatMap(new JoinSplit())
                .collect(toList());

        releaseAll(inputBuffers, "input");
        refCounts(inputBuffers, "input"); // expected; every input has a reference count of 0.

        refCounts(outputBuffers, "outputs"); // expected; reference counts have varied values.
        releaseAll(outputBuffers, "outputs");
        refCounts(outputBuffers, "outputs"); // not expected; every buffer has a reference count of 1.
//        releaseAll(outputBuffers, "outputs"); // try to release the buffers again, and we get a "too many releases" exception.
        if (anyBufferRetained(inputBuffers, "input")) System.exit(-1);
        if (anyBufferRetained(outputBuffers, "outputs")) System.exit(-1); // fails here because at least one buffer is still retained.
    }

    static class JoinSplit implements Function<ByteBuf, Stream<ByteBuf>> {
        ByteBuf current = null;

        @Override
        public Stream<ByteBuf> apply(ByteBuf t) {
            if (current == null) {
                current = t;
            } else {
                current = join(List.of(current, t));
            }
            List<ByteBuf> out = new ArrayList<>();
            while (current.readableBytes() >= 10) {
                out.add(current.retainedSlice(0, 10));
                current = current.slice(10, current.readableBytes()-10);
            }
            return out.stream();
        }
    }

    /**
     * re-implementation of Spring's NettyDataBufferFactory that behaves
     * in the same way but does not use the DataBuffer wrapper.
     */
    static ByteBuf join(List<ByteBuf> parts) {
		if (parts.size() == 1) {
			return parts.get(0);
		}
		CompositeByteBuf composite = BUFFER_FACTORY.compositeBuffer(parts.size());
		for (ByteBuf part : parts) {
			composite.addComponent(true, part);
		}
        return composite;
    }

    static ByteBuf randomBuffer(int length) {
        ByteBuf buff = BUFFER_FACTORY.buffer(length);
        while (length-- > 0) buff.writeByte((byte)(RANDOM.nextInt() & 0xFF));
        return buff;
    }

    static boolean isAllocated(ByteBuf buff) {
        return buff.refCnt() > 0;
    }

    static void refCounts(List<ByteBuf> bufs, String identifier) {
        for (int i = 0; i < bufs.size(); ++i) {
            System.out.println(identifier + " RefCount index: " + i + " -> " + bufs.get(i).refCnt());
        }
    }

    static boolean anyBufferRetained(List<ByteBuf> bufs, String identifier) {
        boolean anyBufferRetained = false;
        for (int i = 0; i < bufs.size(); ++i) {
            ByteBuf get = bufs.get(i);
            System.out.println(identifier + " Checking buffer: " + i + " -> " + (isAllocated(get) ? "allocated" : "unallocated" ) + " (count: " + get.refCnt() + ")");
            anyBufferRetained = anyBufferRetained || isAllocated(get);
        }
        return anyBufferRetained;
    }

    static void releaseAll(List<ByteBuf> bufs, String identifier) {
        for (int i = 0; i < bufs.size(); ++i) {
            System.out.println(identifier + " Releasing index: " + i);
            bufs.get(i).release();
        }
    }

}
