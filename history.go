package main

func (h *QueryHistoryManager) Add(query string) {
	for i, q := range h.history {
		if q == query {
			// Remove the old occurrence
			h.history = append(h.history[:i], h.history[i+1:]...)
			break
		}
	}
	// Append at the end (always)
	h.history = append(h.history, query)
	if len(h.history) > 50 {
		h.history = h.history[len(h.history)-50:]
	}
}

func (h *QueryHistoryManager) clear(a int) string {

	// Convert to 0-based index
	index := a

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
