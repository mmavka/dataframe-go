// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package dataframe

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/olekukonko/tablewriter"
)

// SeriesGeneric is a series of data where the contained data can be
// of any type. Only concrete data types can be used.
type SeriesGeneric struct {
	valFormatter   ValueToStringFormatter
	isEqualFunc    IsEqualFunc
	isLessThanFunc IsLessThanFunc

	concreteType interface{} // The underlying data type

	lock     sync.RWMutex
	name     string
	Values   []interface{}
	nilCount int
}

// NewSeriesGeneric creates a new generic series.
func NewSeriesGeneric(name string, concreteType interface{}, init *SeriesInit, vals ...interface{}) *SeriesGeneric {

	// Validate concrete type
	err := checkConcreteType(concreteType)
	if err != nil {
		panic(err)
	}

	s := &SeriesGeneric{
		isEqualFunc:  DefaultIsEqualFunc,
		name:         name,
		concreteType: concreteType,
		Values:       []interface{}{},
		nilCount:     0,
	}

	var (
		size     int
		capacity int
	)

	if init != nil {
		size = init.Size
		capacity = init.Capacity
		if size > capacity {
			capacity = size
		}
	}

	s.Values = make([]interface{}, size, capacity)
	s.valFormatter = DefaultValueFormatter

	for idx, v := range vals {
		if v != nil {
			if err := s.checkValue(v); err != nil {
				panic(err)
			}
		} else {
			s.nilCount++
		}

		if idx < size {
			s.Values[idx] = v
		} else {
			s.Values = append(s.Values, v)
		}
	}

	if len(vals) < size {
		s.nilCount = s.nilCount + size - len(vals)
	}

	return s
}

// Name returns the series name.
func (s *SeriesGeneric) Name(opts ...Options) string {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	return s.name
}

// Rename renames the series.
func (s *SeriesGeneric) Rename(n string, opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	s.name = n
}

// Type returns the type of data the series holds.
func (s *SeriesGeneric) Type() string {
	return fmt.Sprintf("generic(%T)", s.concreteType)
}

// NRows returns how many rows the series contains.
func (s *SeriesGeneric) NRows(opts ...Options) int {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	return len(s.Values)
}

// Value returns the value of a particular row.
// The return value could be nil or the concrete type
// the data type held by the series.
// Pointers are never returned.
func (s *SeriesGeneric) Value(row int, opts ...Options) interface{} {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	val := s.Values[row]
	if val == nil {
		return nil
	}
	return val
}

func (s *SeriesGeneric) ValueList(opts ...Options) []interface{} {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	out := make([]interface{}, len(s.Values))
	for i, v := range s.Values {
		out[i] = v
	}

	return out
}

// ValueString returns a string representation of a
// particular row. The string representation is defined
// by the function set in SetValueToStringFormatter.
// By default, a nil value is returned as "NaN".
func (s *SeriesGeneric) ValueString(row int, opts ...Options) string {
	return s.valFormatter(s.Value(row, opts...))
}

// Prepend is used to set a value to the beginning of the
// series. val can be a concrete data type or nil. Nil
// represents the absence of a value.
func (s *SeriesGeneric) Prepend(val interface{}, opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	// See: https://stackoverflow.com/questions/41914386/what-is-the-mechanism-of-using-append-to-prepend-in-go

	if cap(s.Values) > len(s.Values) {
		// There is already extra capacity so copy current Values by 1 spot
		s.Values = s.Values[:len(s.Values)+1]
		copy(s.Values[1:], s.Values)
		if val == nil {
			s.Values[0] = nil
		} else {
			if err := s.checkValue(val); err != nil {
				panic(err)
			}
			s.Values[0] = val
		}
		return
	}

	// No room, new slice needs to be allocated:
	s.insert(0, val)
}

// Append is used to set a value to the end of the series.
// val can be a concrete data type or nil. Nil represents
// the absence of a value.
func (s *SeriesGeneric) Append(val interface{}, opts ...Options) int {
	var locked bool
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
		locked = true
	}

	row := s.NRows(Options{DontLock: locked})
	s.insert(row, val)
	return row
}

// Insert is used to set a value at an arbitrary row in
// the series. All existing Values from that row onwards
// are shifted by 1. val can be a concrete data type or nil.
// Nil represents the absence of a value.
func (s *SeriesGeneric) Insert(row int, val interface{}, opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.insert(row, val)
}

func (s *SeriesGeneric) insert(row int, val interface{}) {
	s.Values = append(s.Values, nil)
	copy(s.Values[row+1:], s.Values[row:])

	if val == nil {
		s.nilCount++
		s.Values[row] = nil
	} else {
		if err := s.checkValue(val); err != nil {
			panic(err)
		}
		s.Values[row] = val
	}
}

// Remove is used to delete the value of a particular row.
func (s *SeriesGeneric) Remove(row int, opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	if s.Values[row] == nil {
		s.nilCount--
	}

	s.Values = append(s.Values[:row], s.Values[row+1:]...)
}

// Reset is used clear all data contained in the Series.
func (s *SeriesGeneric) Reset(opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.Values = []interface{}{}
	s.nilCount = 0
}

// Update is used to update the value of a particular row.
// val can be a concrete data type or nil. Nil represents
// the absence of a value.
func (s *SeriesGeneric) Update(row int, val interface{}, opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	if s.Values[row] == nil && val != nil {
		s.nilCount--
	} else if s.Values[row] != nil && val == nil {
		s.nilCount++
	}

	if val == nil {
		s.Values[row] = nil
	} else {
		if err := s.checkValue(val); err != nil {
			panic(err)
		}
		s.Values[row] = val
	}
}

// ValuesIterator will return a function that can be used to iterate through all the Values.
func (s *SeriesGeneric) ValuesIterator(opts ...ValuesOptions) func() (*int, interface{}, int) {

	var (
		row  int
		step int = 1
	)

	var dontReadLock bool

	if len(opts) > 0 {
		dontReadLock = opts[0].DontReadLock

		row = opts[0].InitialRow
		if row < 0 {
			row = len(s.Values) + row
		}
		if opts[0].Step != 0 {
			step = opts[0].Step
		}
	}

	initial := row

	return func() (*int, interface{}, int) {
		if !dontReadLock {
			s.lock.RLock()
			defer s.lock.RUnlock()
		}

		var t int
		if step > 0 {
			t = (len(s.Values)-initial-1)/step + 1
		} else {
			t = -initial/step + 1
		}

		if row > len(s.Values)-1 || row < 0 {
			// Don't iterate further
			return nil, nil, t
		}

		out := s.Values[row]
		if out == nil {
			out = nil
		}
		row = row + step
		return &[]int{row - step}[0], out, t
	}
}

// SetValueToStringFormatter is used to set a function
// to convert the value of a particular row to a string
// representation.
func (s *SeriesGeneric) SetValueToStringFormatter(f ValueToStringFormatter) {
	if f == nil {
		s.valFormatter = DefaultValueFormatter
		return
	}
	s.valFormatter = f
}

// IsEqualFunc returns true if a is equal to b.
func (s *SeriesGeneric) IsEqualFunc(a, b interface{}) bool {

	if s.isEqualFunc == nil {
		panic(errors.New("IsEqualFunc not set"))
	}

	return s.isEqualFunc(a, b)
}

// IsLessThanFunc returns true if a is less than b.
func (s *SeriesGeneric) IsLessThanFunc(a, b interface{}) bool {

	if s.isLessThanFunc == nil {
		panic(errors.New("IsLessThanFunc not set"))
	}

	return s.isLessThanFunc(a, b)
}

// SetIsEqualFunc sets a function which can be used to determine
// if 2 Values in the series are equal.
func (s *SeriesGeneric) SetIsEqualFunc(f IsEqualFunc) {
	if f == nil {
		// Return to default
		s.isEqualFunc = DefaultIsEqualFunc
	} else {
		s.isEqualFunc = f
	}
}

// SetIsLessThanFunc sets a function which can be used to determine
// if a value is less than another in the series.
func (s *SeriesGeneric) SetIsLessThanFunc(f IsLessThanFunc) {
	if f == nil {
		// Return to default
		s.isLessThanFunc = nil
	} else {
		s.isLessThanFunc = f
	}
}

// Sort will sort the series.
// It will return true if sorting was completed or false when the context is canceled.
func (s *SeriesGeneric) Sort(ctx context.Context, opts ...SortOptions) (completed bool) {

	if s.isLessThanFunc == nil {
		panic(fmt.Errorf("cannot sort without setting IsLessThanFunc"))
	}

	defer func() {
		if x := recover(); x != nil {
			completed = false
		}
	}()

	if len(opts) == 0 {
		opts = append(opts, SortOptions{})
	}

	if !opts[0].DontLock {
		s.Lock()
		defer s.Unlock()
	}

	sortFunc := func(i, j int) (ret bool) {
		if err := ctx.Err(); err != nil {
			panic(err)
		}

		defer func() {
			if opts[0].Desc {
				ret = !ret
			}
		}()

		left := s.Values[i]
		right := s.Values[j]

		if left == nil {
			if right == nil {
				// both are nil
				return true
			}
			return true
		}

		if right == nil {
			// left has value and right is nil
			return false
		}
		// Both are not nil
		return s.isLessThanFunc(left, right)
	}

	if opts[0].Stable {
		sort.SliceStable(s.Values, sortFunc)
	} else {
		sort.Slice(s.Values, sortFunc)
	}

	return true
}

// Swap is used to swap 2 Values based on their row position.
func (s *SeriesGeneric) Swap(row1, row2 int, opts ...Options) {
	if row1 == row2 {
		return
	}

	if len(opts) > 0 && !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.Values[row1], s.Values[row2] = s.Values[row2], s.Values[row1]
}

// Lock will lock the Series allowing you to directly manipulate
// the underlying slice with confidence.
func (s *SeriesGeneric) Lock() {
	s.lock.Lock()
}

// Unlock will unlock the Series that was previously locked.
func (s *SeriesGeneric) Unlock() {
	s.lock.Unlock()
}

// Copy will create a new copy of the series.
// It is recommended that you lock the Series before attempting
// to Copy.
func (s *SeriesGeneric) Copy(r ...Range) Series {

	if len(s.Values) == 0 {
		return &SeriesGeneric{
			valFormatter:   s.valFormatter,
			isEqualFunc:    s.isEqualFunc,
			isLessThanFunc: s.isLessThanFunc,

			concreteType: s.concreteType,

			name:     s.name,
			Values:   []interface{}{},
			nilCount: s.nilCount,
		}
	}

	if len(r) == 0 {
		r = append(r, Range{})
	}

	start, end, err := r[0].Limits(len(s.Values))
	if err != nil {
		panic(err)
	}

	// Copy slice
	x := s.Values[start : end+1]
	newSlice := append(x[:0:0], x...)

	return &SeriesGeneric{
		valFormatter:   s.valFormatter,
		isEqualFunc:    s.isEqualFunc,
		isLessThanFunc: s.isLessThanFunc,

		concreteType: s.concreteType,

		name:     s.name,
		Values:   newSlice,
		nilCount: s.nilCount,
	}
}

// Table will produce the Series in a table.
func (s *SeriesGeneric) Table(opts ...TableOptions) string {

	if len(opts) == 0 {
		opts = append(opts, TableOptions{R: &Range{}})
	}

	if !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	data := [][]string{}

	headers := []string{"", s.name} // row header is blank
	footers := []string{fmt.Sprintf("%dx%d", len(s.Values), 1), s.Type()}

	if len(s.Values) > 0 {

		start, end, err := opts[0].R.Limits(len(s.Values))
		if err != nil {
			panic(err)
		}

		for row := start; row <= end; row++ {
			sVals := []string{fmt.Sprintf("%d:", row), s.ValueString(row, dontLock)}
			data = append(data, sVals)
		}

	}

	var buf bytes.Buffer

	table := tablewriter.NewWriter(&buf)
	table.SetHeader(headers)
	for _, v := range data {
		table.Append(v)
	}
	table.SetFooter(footers)
	table.SetAlignment(tablewriter.ALIGN_CENTER)

	table.Render()

	return buf.String()
}

// String implements the fmt.Stringer interface. It does not lock the Series.
func (s *SeriesGeneric) String() string {

	count := len(s.Values)

	out := s.name + ": [ "

	if count > 6 {
		idx := []int{0, 1, 2, count - 3, count - 2, count - 1}
		for j, row := range idx {
			if j == 3 {
				out = out + "... "
			}
			out = out + s.ValueString(row, dontLock) + " "
		}
		return out + "]"
	}

	for row := range s.Values {
		out = out + s.ValueString(row, dontLock) + " "
	}
	return out + "]"
}

// ContainsNil will return whether or not the series contains any nil Values.
func (s *SeriesGeneric) ContainsNil(opts ...Options) bool {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	return s.nilCount > 0
}

// NilCount will return how many nil Values are in the series.
func (s *SeriesGeneric) NilCount(opts ...NilCountOptions) (int, error) {
	if len(opts) == 0 {
		s.lock.RLock()
		defer s.lock.RUnlock()
		return s.nilCount, nil
	}

	if !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	var (
		ctx context.Context
		r   *Range
	)

	if opts[0].Ctx == nil {
		ctx = context.Background()
	} else {
		ctx = opts[0].Ctx
	}

	if opts[0].R == nil {
		r = &Range{}
	} else {
		r = opts[0].R
	}

	start, end, err := r.Limits(len(s.Values))
	if err != nil {
		return 0, err
	}

	if start == 0 && end == len(s.Values)-1 {
		return s.nilCount, nil
	}

	var nilCount int

	for i := start; i <= end; i++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		if s.Values[i] == nil {

			if opts[0].StopAtOneNil {
				return 1, nil
			}

			nilCount++
		}
	}

	return nilCount, nil
}

// ToSeriesMixed will convert the Series to a SeriesMIxed.
// The operation does not lock the Series.
func (s *SeriesGeneric) ToSeriesMixed(ctx context.Context, removeNil bool, conv ...func(interface{}) (interface{}, error)) (*SeriesMixed, error) {
	ec := NewErrorCollection()

	ss := NewSeriesMixed(s.name, &SeriesInit{Capacity: s.NRows(dontLock)})

	for row, rowVal := range s.Values {

		// Cancel operation
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if rowVal == nil {
			if removeNil {
				continue
			}
			ss.Values = append(ss.Values, nil)
			ss.nilCount++
		} else {
			if len(conv) == 0 {
				cv := rowVal
				ss.Values = append(ss.Values, cv)
			} else {
				cv, err := conv[0](rowVal)
				if err != nil {
					// interpret as nil
					ss.Values = append(ss.Values, nil)
					ss.nilCount++
					ec.AddError(&RowError{Row: row, Err: err}, false)
				} else {
					if cv == nil {
						ss.nilCount++
					}
					ss.Values = append(ss.Values, cv)
				}
			}
		}
	}

	if !ec.IsNil(false) {
		return ss, ec
	}

	return ss, nil
}

// IsEqual returns true if s2's Values are equal to s.
func (s *SeriesGeneric) IsEqual(ctx context.Context, s2 Series, opts ...IsEqualOptions) (bool, error) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	// Check type
	gs, ok := s2.(*SeriesGeneric)
	if !ok {
		return false, nil
	}

	// Check number of Values
	if len(s.Values) != len(gs.Values) {
		return false, nil
	}

	// Check name
	if len(opts) != 0 && opts[0].CheckName {
		if s.name != gs.name {
			return false, nil
		}
	}

	// Check Values
	for i, v := range s.Values {
		if err := ctx.Err(); err != nil {
			return false, err
		}

		if v == nil {
			if gs.Values[i] == nil {
				// Both are nil
				continue
			} else {
				return false, nil
			}
		}

		if !s.isEqualFunc(v, gs.Values[i]) {
			return false, nil
		}
	}

	return true, nil
}
