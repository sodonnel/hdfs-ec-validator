package com.sodonnell.benchmarks;

import com.sodonnell.ECValidateUtil;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class BenchmarkBufferAllocate {

  public static void zeroBuffersBB(ByteBuffer[] buf) {
    ByteBuffer newBuf = ByteBuffer.allocate(buf[0].capacity());
    for (ByteBuffer b : buf) {
      b.position(0);
      newBuf.position(0);
      b.put(newBuf);
      b.position(0);
    }
  }

  public static void zeroBuffersByte(ByteBuffer[] buf) {
    for (ByteBuffer b : buf) {
      b.position(0);
      while (b.hasRemaining()) {
        b.put((byte)0);
      }
      b.position(0);
    }
  }

  public static void zeroBuffersByteArray(ByteBuffer[] buf) {
    // Note will not work correctly if the buffer is not an exact multiple of 1024
    byte[] bytes = new byte[1024];
    for (ByteBuffer b : buf) {
      b.position(0);
      while (b.hasRemaining()) {
        b.put(bytes);
      }
      b.position(0);
    }
  }

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    public ByteBuffer[] buffers = ECValidateUtil.allocateBuffers(6, 1024*1024);

    @Setup(Level.Trial)
    public void setUp() {
    }

  }

  public static void main(String[] args) throws Exception {
   // String[] opts = new String[2];
   // opts[0] = "-prof";
   // opts[1] = "gc";
    org.openjdk.jmh.Main.main(args);
  }

  @Benchmark
  @Threads(1)
  @Warmup(iterations = 5, time = 1000, timeUnit = MILLISECONDS)
  @Fork(value = 1, warmups = 0)
  @Measurement(iterations = 5, time = 1000, timeUnit = MILLISECONDS)
  @BenchmarkMode(Mode.Throughput)
  public void allocateNewBuffers(Blackhole blackhole) throws Exception {
    ByteBuffer[] buffers = ECValidateUtil.allocateBuffers(6, 1024*1024);
    blackhole.consume(buffers);
  }

  @Benchmark
  @Threads(1)
  @Warmup(iterations = 5, time = 1000, timeUnit = MILLISECONDS)
  @Fork(value = 1, warmups = 0)
  @Measurement(iterations = 5, time = 1000, timeUnit = MILLISECONDS)
  @BenchmarkMode(Mode.Throughput)
  public void zeroBufferWithBuffer(Blackhole blackhole, BenchmarkState state) throws Exception {
    zeroBuffersBB(state.buffers);
    blackhole.consume(state.buffers);
  }

  @Benchmark
  @Threads(1)
  @Warmup(iterations = 5, time = 1000, timeUnit = MILLISECONDS)
  @Fork(value = 1, warmups = 0)
  @Measurement(iterations = 5, time = 1000, timeUnit = MILLISECONDS)
  @BenchmarkMode(Mode.Throughput)
  public void zeroBufferWithByte(Blackhole blackhole, BenchmarkState state) throws Exception {
    zeroBuffersByte(state.buffers);
    blackhole.consume(state.buffers);
  }

  @Benchmark
  @Threads(1)
  @Warmup(iterations = 5, time = 1000, timeUnit = MILLISECONDS)
  @Fork(value = 1, warmups = 0)
  @Measurement(iterations = 5, time = 1000, timeUnit = MILLISECONDS)
  @BenchmarkMode(Mode.Throughput)
  public void zeroBufferWithByteArray(Blackhole blackhole, BenchmarkState state) throws Exception {
    zeroBuffersByteArray(state.buffers);
    blackhole.consume(state.buffers);
  }

  @Benchmark
  @Threads(1)
  @Warmup(iterations = 5, time = 1000, timeUnit = MILLISECONDS)
  @Fork(value = 1, warmups = 0)
  @Measurement(iterations = 5, time = 1000, timeUnit = MILLISECONDS)
  @BenchmarkMode(Mode.Throughput)
  public void zeroBuffersArray(Blackhole blackhole, BenchmarkState state) throws Exception {
    ECValidateUtil.zeroBuffers(state.buffers);
    blackhole.consume(state.buffers);
  }

  @Benchmark
  @Threads(1)
  @Warmup(iterations = 5, time = 1000, timeUnit = MILLISECONDS)
  @Fork(value = 1, warmups = 0)
  @Measurement(iterations = 5, time = 1000, timeUnit = MILLISECONDS)
  @BenchmarkMode(Mode.Throughput)
  public void resetPosition(Blackhole blackhole, BenchmarkState state) throws Exception {
    ECValidateUtil.resetBufferPosition(state.buffers, 0);
    blackhole.consume(state.buffers);
  }

}