package org.elasticsearch.kafka.consumer.helpers;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionHelper {

    public static String getStrackTraceAsString(final Throwable e) {

        final StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();

    }

}
