package main

import (
	"fmt"
	"strconv"
)

func (h *QueryHistoryManager) Add(query string) {
	for _, q := range h.history {
		if q == query {
			return
		}
	}
	h.history = append([]string{query}, h.history...)
	if len(h.history) > 50 {
		h.history = h.history[:50]
	}
}

func (h *QueryHistoryManager) clear(a string) string {
	i, err := strconv.Atoi(a)
	if err != nil {

		return fmt.Sprintf("Invalid index:", a)
	}

	// Convert to 0-based index
	index := i 

	if index < 0 || index >= len(h.history) {
		return "Index out of range"
	}

	h.history = append(h.history[:index], h.history[index+1:]...)
	return ""
}

func (h *QueryHistoryManager) Navigate(dir int) string {
	h.index += dir
	if h.index < 0 {
		h.index = 0
	} else if h.index >= len(h.history) {
		h.index = len(h.history) - 1
	}
	if len(h.history) == 0 {
		return ""
	}
	return h.history[h.index]
}

func (h *QueryHistoryManager) GetAll() []string {
	return h.history
}
