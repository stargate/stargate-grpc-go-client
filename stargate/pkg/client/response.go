package client

import pb "github.com/stargate/stargate-grpc-go-client/stargate/pkg/proto"

type Response struct {
	ResultSet *ResultSet
	Traces    *Traces
	Warnings  []string
}

type ResultSet struct {
	Columns     []*ColumnSpec
	Rows        []*Row
	PageSize    int32
	PagingState []byte
}

type ColumnSpec struct {
	TypeSpec TypeSpec
	Name     string
}

type Row struct {
	Values []*Value
}

type Traces struct {
	Id        string
	Duration  int64
	StartedAt int64
	Events    []*TracesEvent
}

type TracesEvent struct {
	Activity      string
	Source        string
	SourceElapsed int64
	Thread        string
	EventId       string
}

func translateTraces(traces *pb.Traces) *Traces {
	if traces == nil {
		return nil
	}
	return &Traces{
		Id:        traces.GetId(),
		Duration:  traces.GetDuration(),
		StartedAt: traces.GetStartedAt(),
		Events:    translateEvents(traces.GetEvents()),
	}
}

func translateEvents(protoEvents []*pb.Traces_Event) []*TracesEvent {
	if protoEvents == nil {
		return nil
	}

	var events []*TracesEvent
	for _, event := range protoEvents {
		events = append(events, &TracesEvent{
			Activity:      event.GetActivity(),
			Source:        event.GetSource(),
			SourceElapsed: event.GetSourceElapsed(),
			Thread:        event.GetThread(),
			EventId:       event.GetEventId(),
		})
	}

	return events
}