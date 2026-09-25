package com.github.h3yow.versionedobjectstorejava.store.event;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;

/**
 * 追加写日志中的事件，采用事件溯源记录全部已提交状态变更。
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = SpaceCreatedEvent.class, name = "SpaceCreated"),
        @JsonSubTypes.Type(value = VersionCommittedEvent.class, name = "VersionCommitted"),
        @JsonSubTypes.Type(value = VersionDeletedEvent.class, name = "VersionDeleted")
})
public sealed interface Event
        permits SpaceCreatedEvent, VersionCommittedEvent, VersionDeletedEvent {

    long seq();
}
