package com.github.marschall.jfr.opentelemetry.bridge;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import com.github.marschall.jfr.opentelemetry.bridge.JfrOpentelemetryBridge.ValueExporter.AbstractValueExporter.BooleanValueExporter;
import com.github.marschall.jfr.opentelemetry.bridge.JfrOpentelemetryBridge.ValueExporter.AbstractValueExporter.ByteValueExporter;
import com.github.marschall.jfr.opentelemetry.bridge.JfrOpentelemetryBridge.ValueExporter.AbstractValueExporter.CharValueExporter;
import com.github.marschall.jfr.opentelemetry.bridge.JfrOpentelemetryBridge.ValueExporter.AbstractValueExporter.ClassValueExporter;
import com.github.marschall.jfr.opentelemetry.bridge.JfrOpentelemetryBridge.ValueExporter.AbstractValueExporter.DoubleValueExporter;
import com.github.marschall.jfr.opentelemetry.bridge.JfrOpentelemetryBridge.ValueExporter.AbstractValueExporter.FloatValueExporter;
import com.github.marschall.jfr.opentelemetry.bridge.JfrOpentelemetryBridge.ValueExporter.AbstractValueExporter.IntValueExporter;
import com.github.marschall.jfr.opentelemetry.bridge.JfrOpentelemetryBridge.ValueExporter.AbstractValueExporter.LongValueExporter;
import com.github.marschall.jfr.opentelemetry.bridge.JfrOpentelemetryBridge.ValueExporter.AbstractValueExporter.ObjectValueExporter;
import com.github.marschall.jfr.opentelemetry.bridge.JfrOpentelemetryBridge.ValueExporter.AbstractValueExporter.ShortValueExporter;
import com.github.marschall.jfr.opentelemetry.bridge.JfrOpentelemetryBridge.ValueExporter.AbstractValueExporter.StringValueExporter;
import com.github.marschall.jfr.opentelemetry.bridge.JfrOpentelemetryBridge.ValueExporter.AbstractValueExporter.ThreadValueExporter;
import com.github.marschall.jfr.opentelemetry.bridge.JfrOpentelemetryBridge.ValueExporter.NullValueExporter;

import io.opentelemetry.api.logs.LogRecordBuilder;
import io.opentelemetry.api.logs.Logger;
import jdk.jfr.EventType;
import jdk.jfr.ValueDescriptor;
import jdk.jfr.consumer.EventStream;
import jdk.jfr.consumer.RecordedClass;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedObject;
import jdk.jfr.consumer.RecordedThread;
import jdk.jfr.consumer.RecordingFile;

public final class JfrOpentelemetryBridge implements Consumer<RecordedEvent> {
  // not thread safe

  private static final Set<String> IGNORED_FIELDS = Set.of("name", "startTime");

  private final Logger otelLogger;

  private final Map<Long, EventExporter> exporters;

  public JfrOpentelemetryBridge(Logger otelLogger) {
    this.otelLogger = Objects.requireNonNull(otelLogger);
    this.exporters = new HashMap<>();
  }

  public static void logEventsTo(Path recording, Logger otelLogger) throws IOException {
    JfrOpentelemetryBridge bridge = new JfrOpentelemetryBridge(otelLogger);
    try (RecordingFile recordingFile = new RecordingFile(recording)) {
      while (recordingFile.hasMoreEvents()) {
        RecordedEvent event = recordingFile.readEvent();
        bridge.accept(event);
      }
    }
  }

  public static EventStream streamEventsTo(Path recording, Logger otelLogger) throws IOException {
    JfrOpentelemetryBridge bridge = new JfrOpentelemetryBridge(otelLogger);
    EventStream eventStream = EventStream.openFile(recording);
    eventStream.onEvent(bridge);
    eventStream.setReuse(true);
    eventStream.startAsync();
    return eventStream;
  }

  @Override
  public void accept(RecordedEvent jfrEvent) {
    // missing:
    // - thread
    // - exception and stack trace
    // - category and label
    // missing otel
    // - traceid
    // - spanid
    // - body
    // - severity
    // https://opentelemetry.io/docs/specs/semconv/registry/attributes/jvm/
    EventType eventType = jfrEvent.getEventType();
    
    // end and duration get reported as normal attributes
    LogRecordBuilder builder = this.otelLogger.logRecordBuilder()
        .setEventName(eventType.getName())
        .setTimestamp(jfrEvent.getStartTime());
    RecordedThread thread = jfrEvent.getThread();
    if (thread != null) {
      exportEventThreadTo(thread, builder);
    }

    EventExporter exporter = getEventExporter(eventType);
    exporter.exportEventTo(jfrEvent, builder);

    builder.emit();
  }

  private void exportEventThreadTo(RecordedThread thread, LogRecordBuilder builder) {
    // https://opentelemetry.io/docs/specs/semconv/registry/attributes/thread/
    long threadId = thread.getJavaThreadId();
    String threadName = thread.getJavaName();
    builder.setAttribute("thread.id", threadId);
    builder.setAttribute("thread.name", threadName);
  }

  private EventExporter getEventExporter(EventType eventType) {
    Long eventTypeId = eventType.getId();
    EventExporter exporter = this.exporters.get(eventTypeId);
    if (exporter == null) {
      exporter = buildEventExporter(eventType);
      this.exporters.put(eventTypeId, exporter);
    }
    return exporter;
  }

  private static EventExporter buildEventExporter(EventType eventType) {
    return buildEventExporter(eventType.getFields(), null, new HashSet<>());
  }

  private static EventExporter buildEventExporter(List<ValueDescriptor> fields, String prefix, Set<Long> seen) {
    List<ValueExporter> exporters = new ArrayList<>(fields.size());
    for (ValueDescriptor field : fields) {
      String fieldName = field.getName();
      if (!IGNORED_FIELDS.contains(fieldName)) {
        exporters.add(createExporter(field, prefix, seen));
      }
    }
    return new EventExporter(exporters);
  }


  private static ValueExporter createExporter(ValueDescriptor descriptor, String prefix, Set<Long> seen) {
    String jfrFieldName = descriptor.getName();
    String otelFieldName = prefix == null ? jfrFieldName : prefix + '.' + jfrFieldName;
    String typeName = descriptor.getTypeName();
    return switch (typeName) {
      case "boolean" -> new BooleanValueExporter(otelFieldName, jfrFieldName);
      case "byte" -> new ByteValueExporter(otelFieldName, jfrFieldName);
      case "short" -> new ShortValueExporter(otelFieldName, jfrFieldName);
      case "int" -> new IntValueExporter(otelFieldName, jfrFieldName);
      case "long" -> new LongValueExporter(otelFieldName, jfrFieldName);
      case "char" -> new CharValueExporter(otelFieldName, jfrFieldName);
      case "float" -> new FloatValueExporter(otelFieldName, jfrFieldName);
      case "double" -> new DoubleValueExporter(otelFieldName, jfrFieldName);
      case "java.lang.String" -> new StringValueExporter(otelFieldName, jfrFieldName);
      case "java.lang.Class" -> new ClassValueExporter(otelFieldName, jfrFieldName);
      case "java.lang.Thread" -> new ThreadValueExporter(otelFieldName, jfrFieldName);
      default -> {
        if (typeName.startsWith("jdk.types.")) {
          if (!seen.add(descriptor.getTypeId())) {
            // break loop
            yield NullValueExporter.INSTANCE;
          } else {
            EventExporter delegateExporter = buildEventExporter(descriptor.getFields(), jfrFieldName, seen);
            yield new ObjectValueExporter(otelFieldName, jfrFieldName, delegateExporter);
          }
        } else {
          throw new IllegalStateException("unknown type: " + jfrFieldName);
        }
      }
    };
  }

  static final class EventExporter {

    private final List<ValueExporter> valueExporters;

    EventExporter(List<ValueExporter> valueExporters) {
      this.valueExporters = valueExporters;
    }

    void exportEventTo(RecordedObject jfrEvent, LogRecordBuilder builder) {
      for (ValueExporter exporter : this.valueExporters) {
        exporter.exportValueTo(jfrEvent, builder);
      }
    }
    
    @Override
    public String toString() {
      StringBuilder stringBuilder = new StringBuilder();
      boolean first = true;
      stringBuilder.append('{');
      for (ValueExporter exporter : this.valueExporters) {
        if (!first) {
          stringBuilder.append(", ");
        }
        stringBuilder.append(exporter);
        first = false;
      }
      stringBuilder.append('}');
      return stringBuilder.toString();
    }

  }

  sealed interface ValueExporter {

    void exportValueTo(RecordedObject jfrEvent, LogRecordBuilder builder);
    
    static final class NullValueExporter implements ValueExporter {

      static final ValueExporter INSTANCE = new NullValueExporter();

      private NullValueExporter() {
        super();
      }

      @Override
      public void exportValueTo(RecordedObject jfrEvent, LogRecordBuilder builder) {
        // ignore
      }

      @Override
      public String toString() {
        return "<ignored>";
      }

    }

    static abstract sealed class AbstractValueExporter implements ValueExporter {

      final String otelKey;
      final String jfrKey;

      AbstractValueExporter(String otelKey, String jfrKey) {
        this.otelKey = Objects.requireNonNull(otelKey);
        this.jfrKey = Objects.requireNonNull(jfrKey);
      }
      
      @Override
      public String toString() {
        return this.jfrKey + " (" + stripValueExporter(this.getClass().getSimpleName()) + ")";
      }

      private static String stripValueExporter(String simpleClassName) {
        if (simpleClassName.endsWith("ValueExporter")) {
          return simpleClassName.substring(0, simpleClassName.length() - "ValueExporter".length());
        } else {
          return simpleClassName;
        }
      }

      static final class CharValueExporter extends AbstractValueExporter {

        CharValueExporter(String otelKey, String jfrKey) {
          super(otelKey, jfrKey);
        }

        @Override
        public void exportValueTo(RecordedObject jfrEvent, LogRecordBuilder builder) {
          builder.setAttribute(this.otelKey, jfrEvent.getChar(this.jfrKey));

        }

      }

      static final class StringValueExporter extends AbstractValueExporter {

        StringValueExporter(String otelKey, String jfrKey) {
          super(otelKey, jfrKey);
        }

        @Override
        public void exportValueTo(RecordedObject jfrEvent, LogRecordBuilder builder) {
          builder.setAttribute(this.otelKey, jfrEvent.getString(this.jfrKey));
        }

      }

      static final class ByteValueExporter extends AbstractValueExporter {

        ByteValueExporter(String otelKey, String jfrKey) {
          super(otelKey, jfrKey);
        }

        @Override
        public void exportValueTo(RecordedObject jfrEvent, LogRecordBuilder builder) {
          builder.setAttribute(this.otelKey, jfrEvent.getByte(this.jfrKey));

        }

      }

      static final class ShortValueExporter extends AbstractValueExporter {

        ShortValueExporter(String otelKey, String jfrKey) {
          super(otelKey, jfrKey);
        }

        @Override
        public void exportValueTo(RecordedObject jfrEvent, LogRecordBuilder builder) {
          builder.setAttribute(this.otelKey, jfrEvent.getShort(this.jfrKey));

        }

      }

      static final class IntValueExporter extends AbstractValueExporter {

        IntValueExporter(String otelKey, String jfrKey) {
          super(otelKey, jfrKey);
        }

        @Override
        public void exportValueTo(RecordedObject jfrEvent, LogRecordBuilder builder) {
          builder.setAttribute(this.otelKey, jfrEvent.getInt(this.jfrKey));

        }

      }

      static final class LongValueExporter extends AbstractValueExporter {

        LongValueExporter(String otelKey, String jfrKey) {
          super(otelKey, jfrKey);
        }

        @Override
        public void exportValueTo(RecordedObject jfrEvent, LogRecordBuilder builder) {
          builder.setAttribute(this.otelKey, jfrEvent.getLong(this.jfrKey));

        }

      }

      static final class FloatValueExporter extends AbstractValueExporter {

        FloatValueExporter(String otelKey, String jfrKey) {
          super(otelKey, jfrKey);
        }

        @Override
        public void exportValueTo(RecordedObject jfrEvent, LogRecordBuilder builder) {
          builder.setAttribute(this.otelKey, jfrEvent.getFloat(this.jfrKey));

        }

      }

      static final class DoubleValueExporter extends AbstractValueExporter {

        DoubleValueExporter(String otelKey, String jfrKey) {
          super(otelKey, jfrKey);
        }

        @Override
        public void exportValueTo(RecordedObject jfrEvent, LogRecordBuilder builder) {
          builder.setAttribute(this.otelKey, jfrEvent.getDouble(this.jfrKey));

        }

      }

      static final class BooleanValueExporter extends AbstractValueExporter {

        BooleanValueExporter(String otelKey, String jfrKey) {
          super(otelKey, jfrKey);
        }

        @Override
        public void exportValueTo(RecordedObject jfrEvent, LogRecordBuilder builder) {
          builder.setAttribute(this.otelKey, jfrEvent.getBoolean(this.jfrKey));

        }

      }

      static final class ClassValueExporter extends AbstractValueExporter {

        ClassValueExporter(String otelKey, String jfrKey) {
          super(otelKey, jfrKey);
        }

        @Override
        public void exportValueTo(RecordedObject jfrEvent, LogRecordBuilder builder) {
          RecordedClass clazz = jfrEvent.getClass(this.jfrKey);
          if (clazz != null) {
            builder.setAttribute(this.otelKey, clazz.getName());
          } else {
            builder.setAttribute(this.otelKey, (String) null);
          }
        }

      }

      static final class ObjectValueExporter extends AbstractValueExporter {

        private final EventExporter delegate;

        ObjectValueExporter(String otelKey, String jfrKey, EventExporter delegate) {
          super(otelKey, jfrKey);
          this.delegate = delegate;
        }

        @Override
        public void exportValueTo(RecordedObject jfrObject, LogRecordBuilder builder) {
          Object value = jfrObject.getValue(this.jfrKey);
          if (value instanceof RecordedObject object) {
            this.delegate.exportEventTo(object, builder);
          }
        }

        @Override
        public String toString() {
          return this.jfrKey + " (" + this.delegate + ")";
        }

      }

      static final class ThreadValueExporter extends AbstractValueExporter {

        ThreadValueExporter(String otelKey, String jfrKey) {
          super(otelKey, jfrKey);
        }

        @Override
        public void exportValueTo(RecordedObject jfrEvent, LogRecordBuilder builder) {
          RecordedThread thread = jfrEvent.getThread(this.otelKey);
          if (thread != null) {
            builder.setAttribute(this.otelKey, thread.getJavaName());
          } else {
            builder.setAttribute(this.otelKey, (String) null);
          }
        }

      }

    }

  }

}
