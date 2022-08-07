package org.apache.flink.playgrounds.ops.clickcount;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

import java.util.Date;

@Table(keyspace = "example", name = "clickevent")
public class ClickEventPOJO {
    @Column(name = "timestamp")
    private Date timestamp;

    @Column(name = "page")
    private String page;

    public ClickEventPOJO() {
    }

    public ClickEventPOJO(final Date timestamp, final String page) {
        this.timestamp = timestamp;
        this.page = page;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final Date timestamp) {
        this.timestamp = timestamp;
    }

    public String getPage() {
        return page;
    }

    public void setPage(final String page) {
        this.page = page;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClickEvent{");
        sb.append("timestamp=").append(timestamp);
        sb.append(", page='").append(page).append('\'');
        sb.append('}');
        return sb.toString();
    }



}
