// Code generated by MockGen. DO NOT EDIT.
// Source: subscription_manager.go

// Package testing is a generated GoMock package.
package testing

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	primitive "github.com/linkall-labs/vanus/internal/primitive"
	info "github.com/linkall-labs/vanus/internal/primitive/info"
)

// MockManager is a mock of Manager interface.
type MockManager struct {
	ctrl     *gomock.Controller
	recorder *MockManagerMockRecorder
}

// MockManagerMockRecorder is the mock recorder for MockManager.
type MockManagerMockRecorder struct {
	mock *MockManager
}

// NewMockManager creates a new mock instance.
func NewMockManager(ctrl *gomock.Controller) *MockManager {
	mock := &MockManager{ctrl: ctrl}
	mock.recorder = &MockManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockManager) EXPECT() *MockManagerMockRecorder {
	return m.recorder
}

// AddSubscription mocks base method.
func (m *MockManager) AddSubscription(ctx context.Context, sub *primitive.SubscriptionApi) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddSubscription", ctx, sub)
	ret0, _ := ret[0].(error)
	return ret0
}

// AddSubscription indicates an expected call of AddSubscription.
func (mr *MockManagerMockRecorder) AddSubscription(ctx, sub interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddSubscription", reflect.TypeOf((*MockManager)(nil).AddSubscription), ctx, sub)
}

// DeleteSubscription mocks base method.
func (m *MockManager) DeleteSubscription(ctx context.Context, subId string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteSubscription", ctx, subId)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteSubscription indicates an expected call of DeleteSubscription.
func (mr *MockManagerMockRecorder) DeleteSubscription(ctx, subId interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteSubscription", reflect.TypeOf((*MockManager)(nil).DeleteSubscription), ctx, subId)
}

// GetOffset mocks base method.
func (m *MockManager) GetOffset(ctx context.Context, subId string) (info.ListOffsetInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetOffset", ctx, subId)
	ret0, _ := ret[0].(info.ListOffsetInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetOffset indicates an expected call of GetOffset.
func (mr *MockManagerMockRecorder) GetOffset(ctx, subId interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetOffset", reflect.TypeOf((*MockManager)(nil).GetOffset), ctx, subId)
}

// GetSubscription mocks base method.
func (m *MockManager) GetSubscription(ctx context.Context, subId string) *primitive.SubscriptionApi {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSubscription", ctx, subId)
	ret0, _ := ret[0].(*primitive.SubscriptionApi)
	return ret0
}

// GetSubscription indicates an expected call of GetSubscription.
func (mr *MockManagerMockRecorder) GetSubscription(ctx, subId interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSubscription", reflect.TypeOf((*MockManager)(nil).GetSubscription), ctx, subId)
}

// Init mocks base method.
func (m *MockManager) Init(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Init", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Init indicates an expected call of Init.
func (mr *MockManagerMockRecorder) Init(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Init", reflect.TypeOf((*MockManager)(nil).Init), ctx)
}

// ListSubscription mocks base method.
func (m *MockManager) ListSubscription(ctx context.Context) map[string]*primitive.SubscriptionApi {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListSubscription", ctx)
	ret0, _ := ret[0].(map[string]*primitive.SubscriptionApi)
	return ret0
}

// ListSubscription indicates an expected call of ListSubscription.
func (mr *MockManagerMockRecorder) ListSubscription(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListSubscription", reflect.TypeOf((*MockManager)(nil).ListSubscription), ctx)
}

// Offset mocks base method.
func (m *MockManager) Offset(ctx context.Context, subInfo info.SubscriptionInfo) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Offset", ctx, subInfo)
	ret0, _ := ret[0].(error)
	return ret0
}

// Offset indicates an expected call of Offset.
func (mr *MockManagerMockRecorder) Offset(ctx, subInfo interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Offset", reflect.TypeOf((*MockManager)(nil).Offset), ctx, subInfo)
}

// Start mocks base method.
func (m *MockManager) Start() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Start")
}

// Start indicates an expected call of Start.
func (mr *MockManagerMockRecorder) Start() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockManager)(nil).Start))
}

// Stop mocks base method.
func (m *MockManager) Stop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop")
}

// Stop indicates an expected call of Stop.
func (mr *MockManagerMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockManager)(nil).Stop))
}

// UpdateSubscription mocks base method.
func (m *MockManager) UpdateSubscription(ctx context.Context, sub *primitive.SubscriptionApi) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateSubscription", ctx, sub)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateSubscription indicates an expected call of UpdateSubscription.
func (mr *MockManagerMockRecorder) UpdateSubscription(ctx, sub interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateSubscription", reflect.TypeOf((*MockManager)(nil).UpdateSubscription), ctx, sub)
}