// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package dataframe

import (
	"bytes"
	"context"
	"fmt"
	"golang.org/x/exp/rand"
	"sort"
	"strconv"
	"sync"

	"github.com/olekukonko/tablewriter"
)

// SeriesString is used for series containing string data.
type SeriesString struct {
	valFormatter ValueToStringFormatter

	lock     sync.RWMutex
	name     string
	Values   []*string
	nilCount int
}

// NewSeriesString creates a new series with the underlying type as string.
func NewSeriesString(name string, init *SeriesInit, vals ...interface{}) *SeriesString {
	s := &SeriesString{
		name:     name,
		Values:   []*string{},
		nilCount: 0,
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

	s.Values = make([]*string, size, capacity)
	s.valFormatter = DefaultValueFormatter

	for idx, v := range vals {

		// Special case
		if idx == 0 {
			if ss, ok := vals[0].([]string); ok {
				for idx, v := range ss {
					val := s.valToPointer(v)
					if idx < size {
						s.Values[idx] = val
					} else {
						s.Values = append(s.Values, val)
					}
				}
				break
			}
		}

		val := s.valToPointer(v)
		if val == nil {
			s.nilCount++
		}

		if idx < size {
			s.Values[idx] = val
		} else {
			s.Values = append(s.Values, val)
		}
	}

	var lVals int
	if len(vals) > 0 {
		if ss, ok := vals[0].([]string); ok {
			lVals = len(ss)
		} else {
			lVals = len(vals)
		}
	}

	if lVals < size {
		s.nilCount = s.nilCount + size - lVals
	}

	return s
}

// NewSeries creates a new initialized SeriesString.
func (s *SeriesString) NewSeries(name string, init *SeriesInit) Series {
	return NewSeriesString(name, init)
}

// Name returns the series name.
func (s *SeriesString) Name(opts ...Options) string {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	return s.name
}

// Rename renames the series.
func (s *SeriesString) Rename(n string, opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	s.name = n
}

// Type returns the type of data the series holds.
func (s *SeriesString) Type() string {
	return "string"
}

// NRows returns how many rows the series contains.
func (s *SeriesString) NRows(opts ...Options) int {
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
func (s *SeriesString) Value(row int, opts ...Options) interface{} {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	val := s.Values[row]
	if val == nil {
		return nil
	}
	return *val
}

// ValueString returns a string representation of a
// particular row. The string representation is defined
// by the function set in SetValueToStringFormatter.
// By default, a nil value is returned as "NaN".
func (s *SeriesString) ValueString(row int, opts ...Options) string {
	return s.valFormatter(s.Value(row, opts...))
}

// Prepend is used to set a value to the beginning of the
// series. val can be a concrete data type or nil. Nil
// represents the absence of a value.
func (s *SeriesString) Prepend(val interface{}, opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	// See: https://stackoverflow.com/questions/41914386/what-is-the-mechanism-of-using-append-to-prepend-in-go

	if cap(s.Values) > len(s.Values) {
		// There is already extra capacity so copy current Values by 1 spot
		s.Values = s.Values[:len(s.Values)+1]
		copy(s.Values[1:], s.Values)
		s.Values[0] = s.valToPointer(val)
		return
	}

	// No room, new slice needs to be allocated:
	s.insert(0, val)
}

// Append is used to set a value to the end of the series.
// val can be a concrete data type or nil. Nil represents
// the absence of a value.
func (s *SeriesString) Append(val interface{}, opts ...Options) int {
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
func (s *SeriesString) Insert(row int, val interface{}, opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.insert(row, val)
}

func (s *SeriesString) insert(row int, val interface{}) {
	switch V := val.(type) {
	case []string:
		var vals []*string
		for _, v := range V {
			v := v
			vals = append(vals, &v)
		}
		s.Values = append(s.Values[:row], append(vals, s.Values[row:]...)...)
		return
	case []*string:
		for _, v := range V {
			if v == nil {
				s.nilCount++
			}
		}
		s.Values = append(s.Values[:row], append(V, s.Values[row:]...)...)
		return
	}

	s.Values = append(s.Values, nil)
	copy(s.Values[row+1:], s.Values[row:])

	v := s.valToPointer(val)
	if v == nil {
		s.nilCount++
	}

	s.Values[row] = s.valToPointer(v)
}

// Remove is used to delete the value of a particular row.
func (s *SeriesString) Remove(row int, opts ...Options) {
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
func (s *SeriesString) Reset(opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.Values = []*string{}
	s.nilCount = 0
}

// Update is used to update the value of a particular row.
// val can be a concrete data type or nil. Nil represents
// the absence of a value.
func (s *SeriesString) Update(row int, val interface{}, opts ...Options) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	newVal := s.valToPointer(val)

	if s.Values[row] == nil && newVal != nil {
		s.nilCount--
	} else if s.Values[row] != nil && newVal == nil {
		s.nilCount++
	}

	s.Values[row] = newVal
}

// ValuesIterator will return a function that can be used to iterate through all the Values.
func (s *SeriesString) ValuesIterator(opts ...ValuesOptions) func() (*int, interface{}, int) {

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

		val := s.Values[row]
		var out interface{}
		if val == nil {
			out = nil
		} else {
			out = *val
		}
		row = row + step
		return &[]int{row - step}[0], out, t
	}
}

func (s *SeriesString) valToPointer(v interface{}) *string {
	switch val := v.(type) {
	case nil:
		return nil
	case *bool:
		if val == nil {
			return nil
		}
		if *val == true {
			return &[]string{"true"}[0]
		} else {
			return &[]string{"false"}[0]
		}
	case bool:
		if val == true {
			return &[]string{"true"}[0]
		} else {
			return &[]string{"false"}[0]
		}
	case *string:
		if val == nil {
			return nil
		}
		return &[]string{*val}[0]
	case string:
		return &val
	case *float64:
		if val == nil {
			return nil
		}
		return &[]string{strconv.FormatFloat(*val, 'G', -1, 64)}[0]
	case float64:
		return &[]string{strconv.FormatFloat(val, 'G', -1, 64)}[0]
	case *float32:
		if val == nil {
			return nil
		}
		return &[]string{strconv.FormatFloat(float64(*val), 'G', -1, 64)}[0]
	case float32:
		return &[]string{strconv.FormatFloat(float64(val), 'G', -1, 64)}[0]
	case *int64:
		if val == nil {
			return nil
		}
		return &[]string{strconv.FormatInt(*val, 10)}[0]
	case int64:
		return &[]string{strconv.FormatInt(val, 10)}[0]
	case *int:
		if val == nil {
			return nil
		}
		return &[]string{strconv.Itoa(*val)}[0]
	case int:
		return &[]string{strconv.Itoa(val)}[0]
	case *int32:
		if val == nil {
			return nil
		}
		return &[]string{strconv.FormatInt(int64(*val), 10)}[0]
	case int32:
		return &[]string{strconv.FormatInt(int64(val), 10)}[0]
	default:
		_ = v.(string) // Intentionally panic
		return nil
	}
}

// SetValueToStringFormatter is used to set a function
// to convert the value of a particular row to a string
// representation.
func (s *SeriesString) SetValueToStringFormatter(f ValueToStringFormatter) {
	if f == nil {
		s.valFormatter = DefaultValueFormatter
		return
	}
	s.valFormatter = f
}

// Swap is used to swap 2 Values based on their row position.
func (s *SeriesString) Swap(row1, row2 int, opts ...Options) {
	if row1 == row2 {
		return
	}

	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	s.Values[row1], s.Values[row2] = s.Values[row2], s.Values[row1]
}

// IsEqualFunc returns true if a is equal to b.
func (s *SeriesString) IsEqualFunc(a, b interface{}) bool {

	if a == nil {
		if b == nil {
			return true
		}
		return false
	}

	if b == nil {
		return false
	}
	s1 := a.(string)
	s2 := b.(string)

	return s1 == s2

}

// IsLessThanFunc returns true if a is less than b.
func (s *SeriesString) IsLessThanFunc(a, b interface{}) bool {

	if a == nil {
		if b == nil {
			return true
		}
		return true
	}

	if b == nil {
		return false
	}
	s1 := a.(string)
	s2 := b.(string)

	return s1 < s2

}

// Sort will sort the series.
// It will return true if sorting was completed or false when the context is canceled.
func (s *SeriesString) Sort(ctx context.Context, opts ...SortOptions) (completed bool) {

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

		if s.Values[i] == nil {
			if s.Values[j] == nil {
				// both are nil
				return true
			}
			return true
		}

		if s.Values[j] == nil {
			// i has value and j is nil
			return false
		}
		// Both are not nil
		ti := *s.Values[i]
		tj := *s.Values[j]

		return ti < tj
	}

	if opts[0].Stable {
		sort.SliceStable(s.Values, sortFunc)
	} else {
		sort.Slice(s.Values, sortFunc)
	}

	return true
}

// Lock will lock the Series allowing you to directly manipulate
// the underlying slice with confidence.
func (s *SeriesString) Lock() {
	s.lock.Lock()
}

// Unlock will unlock the Series that was previously locked.
func (s *SeriesString) Unlock() {
	s.lock.Unlock()
}

// Copy will create a new copy of the series.
// It is recommended that you lock the Series before attempting
// to Copy.
func (s *SeriesString) Copy(r ...Range) Series {

	if len(s.Values) == 0 {
		return &SeriesString{
			valFormatter: s.valFormatter,
			name:         s.name,
			Values:       []*string{},
			nilCount:     s.nilCount,
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

	return &SeriesString{
		valFormatter: s.valFormatter,
		name:         s.name,
		Values:       newSlice,
		nilCount:     s.nilCount,
	}
}

// Table will produce the Series in a table.
func (s *SeriesString) Table(opts ...TableOptions) string {

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
func (s *SeriesString) String() string {

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
	} else {
		for row := range s.Values {
			out = out + s.ValueString(row, dontLock) + " "
		}
		return out + "]"
	}
}

// ContainsNil will return whether or not the series contains any nil Values.
func (s *SeriesString) ContainsNil(opts ...Options) bool {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	return s.nilCount > 0
}

// NilCount will return how many nil Values are in the series.
func (s *SeriesString) NilCount(opts ...NilCountOptions) (int, error) {
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

// ToSeriesInt64 will convert the Series to a SeriesInt64.
// The operation does not lock the Series.
func (s *SeriesString) ToSeriesInt64(ctx context.Context, removeNil bool, conv ...func(interface{}) (*int64, error)) (*SeriesInt64, error) {

	ec := NewErrorCollection()

	ss := NewSeriesInt64(s.name, &SeriesInit{Capacity: s.NRows(dontLock)})

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
				cv, err := strconv.ParseInt(*rowVal, 10, 64)
				if err != nil {
					// interpret as nil
					ss.Values = append(ss.Values, nil)
					ss.nilCount++
					ec.AddError(&RowError{Row: row, Err: err}, false)
				} else {
					ss.Values = append(ss.Values, &cv)
				}
			} else {
				cv, err := conv[0](rowVal)
				if err != nil {
					// interpret as nil
					ss.Values = append(ss.Values, nil)
					ss.nilCount++
					ec.AddError(&RowError{Row: row, Err: err}, false)
				} else {
					if cv == nil {
						ss.Values = append(ss.Values, nil)
						ss.nilCount++
					} else {
						ss.Values = append(ss.Values, cv)
					}
				}
			}
		}
	}

	if !ec.IsNil(false) {
		return ss, ec
	}

	return ss, nil
}

// ToSeriesFloat64 will convert the Series to a SeriesFloat64.
// The operation does not lock the Series.
func (s *SeriesString) ToSeriesFloat64(ctx context.Context, removeNil bool, conv ...func(interface{}) (float64, error)) (*SeriesFloat64, error) {

	ec := NewErrorCollection()

	ss := NewSeriesFloat64(s.name, &SeriesInit{Capacity: s.NRows(dontLock)})

	for row, rowVal := range s.Values {

		// Cancel operation
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if rowVal == nil {
			if removeNil {
				continue
			}
			ss.Values = append(ss.Values, nan())
			ss.nilCount++
		} else {
			if len(conv) == 0 {
				cv, err := strconv.ParseFloat(*rowVal, 64)
				if err != nil {
					// interpret as nil
					ss.Values = append(ss.Values, nan())
					ss.nilCount++
					ec.AddError(&RowError{Row: row, Err: err}, false)
				} else {
					ss.Values = append(ss.Values, cv)
				}
			} else {
				cv, err := conv[0](rowVal)
				if err != nil {
					// interpret as nil
					ss.Values = append(ss.Values, nan())
					ss.nilCount++
					ec.AddError(&RowError{Row: row, Err: err}, false)
				} else {
					if isNaN(cv) {
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

// ToSeriesMixed will convert the Series to a SeriesMIxed.
// The operation does not lock the Series.
func (s *SeriesString) ToSeriesMixed(ctx context.Context, removeNil bool, conv ...func(interface{}) (interface{}, error)) (*SeriesMixed, error) {
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
				cv := *rowVal
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

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomString(rng *rand.Rand) *string {
	b := make([]byte, 12)
	for i := range b {
		b[i] = charset[rng.Intn(len(charset))]
	}
	return &[]string{string(b)}[0]
}

// FillRand will fill a Series with random data. probNil is a value between between 0 and 1 which
// determines if a row is given a nil value.
func (s *SeriesString) FillRand(src rand.Source, probNil float64, rander Rander, opts ...FillRandOptions) {

	rng := rand.New(src)

	capacity := cap(s.Values)
	length := len(s.Values)
	s.nilCount = 0

	for i := 0; i < length; i++ {
		if rng.Float64() < probNil {
			// nil
			s.Values[i] = nil
			s.nilCount++
		} else {
			s.Values[i] = randomString(rng)
		}
	}

	if capacity > length {
		excess := capacity - length
		for i := 0; i < excess; i++ {
			if rng.Float64() < probNil {
				// nil
				s.Values = append(s.Values, nil)
				s.nilCount++
			} else {
				s.Values = append(s.Values, randomString(rng))
			}
		}
	}
}

// IsEqual returns true if s2's Values are equal to s.
func (s *SeriesString) IsEqual(ctx context.Context, s2 Series, opts ...IsEqualOptions) (bool, error) {
	if len(opts) == 0 || !opts[0].DontLock {
		s.lock.RLock()
		defer s.lock.RUnlock()
	}

	// Check type
	ss, ok := s2.(*SeriesString)
	if !ok {
		return false, nil
	}

	// Check number of Values
	if len(s.Values) != len(s.Values) {
		return false, nil
	}

	// Check name
	if len(opts) != 0 && opts[0].CheckName {
		if s.name != ss.name {
			return false, nil
		}
	}

	// Check Values
	for i, v := range s.Values {
		if err := ctx.Err(); err != nil {
			return false, err
		}

		if v == nil {
			if ss.Values[i] == nil {
				// Both are nil
				continue
			} else {
				return false, nil
			}
		}

		if *v != *ss.Values[i] {
			return false, nil
		}
	}

	return true, nil
}
