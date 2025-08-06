package main

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

func (h *QueryHistoryManager) Navigate(dir int) string {
	h.index += dir
	if h.index < 0 {
		h.index = 0
	} else if h.index >= len(h.history) {
		h.index = len(h.history) - 1
	}
	return h.history[h.index]
}

func (h *QueryHistoryManager) GetAll() []string {
	return h.history
}

