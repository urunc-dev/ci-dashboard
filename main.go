package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

type Config struct {
	Settings struct {
		SourceRepo         string `yaml:"source_repo"`
		MaxRunsPerWorkflow int    `yaml:"max_runs_per_workflow"`
		RecentRunsInOutput int    `yaml:"recent_runs_in_output"`
	} `yaml:"settings"`

	LogAnalysis LogAnalysisConfig `yaml:"log_analysis"`

	Workflows []struct {
		Name        string `yaml:"name"`
		Description string `yaml:"description"`
		Critical    bool   `yaml:"critical"`
		Required    bool   `yaml:"required"`
	} `yaml:"workflows"`
}

type LogAnalysisConfig struct {
	MaxSignalsPerJob int                     `yaml:"max_signals_per_job"`
	NoisePatterns    []string                `yaml:"noise_patterns"`
	Categories       []FailureCategoryConfig `yaml:"categories"`
}

type FailureCategoryConfig struct {
	Name     string   `yaml:"name"`
	Priority int      `yaml:"priority"`
	Patterns []string `yaml:"patterns"`
}

type SignalLine struct {
	Line     string
	Category string
	Priority int
}

type LogSummary struct {
	Signals     []SignalLine
	ByCategory  map[string][]string
	TopCategory string
	Empty       bool
}

type Workflow struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Path string `json:"path"`
}

type WorkflowsListResponse struct {
	Workflows []Workflow `json:"workflows"`
}

type WorkflowsResponse struct {
	WorkflowRuns []Run `json:"workflow_runs"`
}

type FailedJob struct {
	ID         int    `json:"id"`
	Name       string `json:"name"`
	Conclusion string `json:"conclusion"`
	HTMLURL    string `json:"html_url"`
	LogSnippet string `json:"log_snippet"`
}

type Run struct {
	ID           int          `json:"id"`
	Name         string       `json:"name"`
	Status       string       `json:"status"`
	Conclusion   string       `json:"conclusion"`
	RunNumber    int          `json:"run_number"`
	CreatedAt    time.Time    `json:"created_at"`
	UpdatedAt    time.Time    `json:"updated_at"`
	RunStartedAt time.Time    `json:"run_started_at"`
	HTMLURL      string       `json:"html_url"`
	Jobs         []JobSummary `json:"jobs,omitempty"`
	FailedJobs   []FailedJob  `json:"failed_jobs,omitempty"`
	RunAttempt   int          `json:"run_attempt"`
}

type Job struct {
	ID          int       `json:"id"`
	Name        string    `json:"name"`
	Status      string    `json:"status"`
	Conclusion  string    `json:"conclusion"`
	HTMLURL     string    `json:"html_url"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
}

type JobSummary struct {
	Name        string  `json:"name"`
	Conclusion  string  `json:"conclusion"`
	DurationSec float64 `json:"duration_sec"`
	HTMLURL     string  `json:"html_url"`
}

type JobsResponse struct {
	Jobs []Job `json:"jobs"`
}

type WorkflowSummary struct {
	Name            string   `json:"name"`
	Description     string   `json:"description"`
	Critical        bool     `json:"critical"`
	Required        bool     `json:"required"`
	TotalRuns       int      `json:"total_runs"`
	FailedRuns      int      `json:"failed_runs"`
	FailureRate     float64  `json:"failure_rate"`
	AvgDurationSecs float64  `json:"avg_duration_secs"`
	WeatherHistory  []string `json:"weather_history"`
	LastRun         *Run     `json:"last_run"`
	RecentRuns      []Run    `json:"recent_runs"`
}

type DashboardData struct {
	GeneratedAt   time.Time         `json:"generated_at"`
	Repo          string            `json:"repo"`
	OverallHealth float64           `json:"overall_health"`
	Workflows     []WorkflowSummary `json:"workflows"`
}

type Client struct {
	token       string
	repo        string
	httpClient  *http.Client
	logClient   *http.Client
	logAnalysis LogAnalysisConfig
}

func NewClient(token, repo string, la LogAnalysisConfig) *Client {
	return &Client{
		token:       token,
		repo:        repo,
		httpClient:  &http.Client{Timeout: 20 * time.Second},
		logClient:   &http.Client{Timeout: 2 * time.Minute},
		logAnalysis: la,
	}
}

func (c *Client) get(url string, v interface{}) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	req.Header.Set("Accept", "application/vnd.github+json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("GitHub API returned HTTP %d for %s", resp.StatusCode, url)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

func analyseLog(scanner *bufio.Scanner, cfg LogAnalysisConfig) LogSummary {
	maxSignals := cfg.MaxSignalsPerJob
	if maxSignals <= 0 {
		maxSignals = 20
	}
	var signals []SignalLine
	seen := make(map[string]bool)
	for scanner.Scan() {
		if len(signals) >= maxSignals {
			break
		}
		clean := stripGHTimestamp(scanner.Text())
		if clean == "" {
			continue
		}
		lower := strings.ToLower(clean)
		if matchesAny(lower, cfg.NoisePatterns) {
			continue
		}
		for _, cat := range cfg.Categories {
			if matchesAny(lower, cat.Patterns) && !seen[clean] {
				seen[clean] = true
				signals = append(signals, SignalLine{
					Line:     clean,
					Category: cat.Name,
					Priority: cat.Priority,
				})
				break
			}
		}
	}

	if len(signals) == 0 {
		return LogSummary{Empty: true}
	}

	sort.SliceStable(signals, func(i, j int) bool {
		return signals[i].Priority < signals[j].Priority
	})
	topCategory := signals[0].Category

	byCategory := make(map[string][]string)
	for _, s := range signals {
		byCategory[s.Category] = append(byCategory[s.Category], s.Line)
	}

	return LogSummary{
		Signals:     signals,
		ByCategory:  byCategory,
		TopCategory: topCategory,
	}
}

func renderSummary(s LogSummary) string {
	if s.Empty {
		return "(no actionable failure signal found in log)"
	}

	total := len(s.Signals)
	var sb strings.Builder

	fmt.Fprintf(&sb, "[%s] — %d signal(s) found\n", s.TopCategory, total)
	sb.WriteString(strings.Repeat("─", 48) + "\n")

	seen := make(map[string]bool)
	var orderedCats []string
	for _, sig := range s.Signals {
		if !seen[sig.Category] {
			seen[sig.Category] = true
			orderedCats = append(orderedCats, sig.Category)
		}
	}

	for _, cat := range orderedCats {
		fmt.Fprintf(&sb, "-> %s\n", cat)
		for _, line := range s.ByCategory[cat] {
			fmt.Fprintf(&sb, "  %s\n", line)
		}
	}

	return strings.TrimRight(sb.String(), "\n")
}

func matchesAny(lower string, patterns []string) bool {
	for _, p := range patterns {
		if strings.Contains(lower, strings.ToLower(p)) {
			return true
		}
	}
	return false
}

// fetchAndAnalyseLog streams the log response directly into the analyser
func (c *Client) fetchAndAnalyseLog(logURL string) (string, error) {
	var (
		resp *http.Response
		err  error
	)
	for attempt := 0; attempt < 2; attempt++ {
		req, _ := http.NewRequest("GET", logURL, nil)
		if c.token != "" {
			req.Header.Set("Authorization", "Bearer "+c.token)
		}
		req.Header.Set("Accept", "application/vnd.github+json")
		resp, err = c.logClient.Do(req)
		if err == nil {
			break
		}
		if attempt == 0 {
			log.Printf("log fetch attempt 1 failed (%v), retrying...", err)
			time.Sleep(3 * time.Second)
		}
	}
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("log fetch returned HTTP %d", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 256*1024), 256*1024)
	summary := analyseLog(scanner, c.logAnalysis)
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return renderSummary(summary), nil
}

func (c *Client) fetchJobSummaries(run *Run) {
	url := fmt.Sprintf("https://api.github.com/repos/%s/actions/runs/%d/jobs", c.repo, run.ID)
	var resp JobsResponse
	if err := c.get(url, &resp); err != nil {
		log.Printf("warn: could not fetch jobs for run %d: %v", run.ID, err)
		return
	}
	for _, job := range resp.Jobs {
		dur := job.CompletedAt.Sub(job.StartedAt).Seconds()
		if dur < 0 {
			dur = 0
		}
		run.Jobs = append(run.Jobs, JobSummary{
			Name:        job.Name,
			Conclusion:  job.Conclusion,
			DurationSec: dur,
			HTMLURL:     job.HTMLURL,
		})
	}
}

func (c *Client) enrichWithLogs(run *Run) {
	url := fmt.Sprintf("https://api.github.com/repos/%s/actions/runs/%d/jobs", c.repo, run.ID)
	var resp JobsResponse
	if err := c.get(url, &resp); err != nil {
		log.Printf("warn: could not fetch jobs for run %d: %v", run.ID, err)
		return
	}
	for _, job := range resp.Jobs {
		if job.Conclusion != "failure" {
			continue
		}
		logURL := fmt.Sprintf("https://api.github.com/repos/%s/actions/jobs/%d/logs", c.repo, job.ID)
		log.Printf("fetching logs for failed job %d (%s)...", job.ID, job.Name)
		snippet, err := c.fetchAndAnalyseLog(logURL)
		if err != nil {
			log.Printf("warn: could not fetch logs for job %d: %v", job.ID, err)
			snippet = "(log fetch failed)"
		}
		run.FailedJobs = append(run.FailedJobs, FailedJob{
			ID:         job.ID,
			Name:       job.Name,
			Conclusion: job.Conclusion,
			HTMLURL:    job.HTMLURL,
			LogSnippet: snippet,
		})
	}
}

func stripGHTimestamp(line string) string {
	if len(line) > 29 && line[10] == 'T' {
		line = line[29:]
	}
	return strings.TrimSpace(line)
}

func normalize(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, "-", "")
	s = strings.ReplaceAll(s, "_", "")
	s = strings.ReplaceAll(s, " ", "")
	return s
}

func stemPath(path string) string {
	parts := strings.Split(path, "/")
	file := parts[len(parts)-1]
	file = strings.TrimSuffix(file, ".yml")
	file = strings.TrimSuffix(file, ".yaml")
	return normalize(file)
}

func findWorkflow(workflows []Workflow, keyword string) *Workflow {
	key := normalize(keyword)
	for i, wf := range workflows {
		if stemPath(wf.Path) == key {
			return &workflows[i]
		}
	}
	for i, wf := range workflows {
		stem := stemPath(wf.Path)
		if strings.Contains(stem, key) || strings.Contains(key, stem) {
			return &workflows[i]
		}
	}
	for i, wf := range workflows {
		name := normalize(wf.Name)
		if strings.Contains(name, key) || strings.Contains(key, name) {
			return &workflows[i]
		}
	}
	return nil
}

func buildWeatherHistory(runs []Run) []string {
	const slots = 7
	history := make([]string, slots)
	for i := range history {
		history[i] = "unknown"
	}
	take := runs
	if len(take) > slots {
		take = runs[:slots]
	}

	offset := slots - len(take)
	for i, r := range take {
		idx := len(take) - 1 - i
		c := r.Conclusion
		if c == "" {
			c = "unknown"
		}
		switch c {
		case "success", "failure", "skipped", "action_required":
		default:
			c = "unknown"
		}
		history[offset+idx] = c
	}
	return history
}

func buildSummary(runs []Run, name, desc string, critical, required bool) WorkflowSummary {
	var failed int
	var totalDuration float64
	for _, r := range runs {
		if r.Conclusion == "failure" {
			failed++
		}
		start := r.RunStartedAt
		if start.IsZero() {
			start = r.CreatedAt
		}
		totalDuration += r.UpdatedAt.Sub(start).Seconds()
	}
	total := len(runs)
	var failureRate, avg float64
	if total > 0 {
		failureRate = float64(failed) / float64(total) * 100
		avg = totalDuration / float64(total)
	}
	var lastRun *Run
	if len(runs) > 0 {
		r := runs[0]
		lastRun = &r
	}
	return WorkflowSummary{
		Name:            name,
		Description:     desc,
		Critical:        critical,
		Required:        required,
		TotalRuns:       total,
		FailedRuns:      failed,
		FailureRate:     failureRate,
		AvgDurationSecs: avg,
		WeatherHistory:  buildWeatherHistory(runs),
		LastRun:         lastRun,
		RecentRuns:      runs,
	}
}

func main() {
	cfgBytes, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatalf("cannot read config.yaml: %v", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(cfgBytes, &cfg); err != nil {
		log.Fatalf("cannot parse config.yaml: %v", err)
	}

	recentLimit := cfg.Settings.RecentRunsInOutput
	if recentLimit <= 0 {
		recentLimit = cfg.Settings.MaxRunsPerWorkflow
	}

	workflowsBytes, err := os.ReadFile("workflows_raw.json")
	if err != nil {
		log.Fatalf("cannot read workflows_raw.json: %v", err)
	}
	var workflowsResp WorkflowsListResponse
	if err := json.Unmarshal(workflowsBytes, &workflowsResp); err != nil {
		log.Fatalf("cannot parse workflows_raw.json: %v", err)
	}

	runsBytes, err := os.ReadFile("runs_raw.json")
	if err != nil {
		log.Fatalf("cannot read runs_raw.json: %v", err)
	}
	var allRuns map[string]WorkflowsResponse
	if err := json.Unmarshal(runsBytes, &allRuns); err != nil {
		log.Fatalf("cannot parse runs_raw.json: %v", err)
	}

	client := NewClient(os.Getenv("GITHUB_TOKEN"), cfg.Settings.SourceRepo, cfg.LogAnalysis)

	var summaries []WorkflowSummary
	var totalHealth float64

	for _, w := range cfg.Workflows {
		wf := findWorkflow(workflowsResp.Workflows, w.Name)
		if wf == nil {
			log.Println("Not found:", w.Name)
			continue
		}
		log.Printf("Matched: %s -> %s", w.Name, wf.Name)

		runsData, ok := allRuns[fmt.Sprintf("%d", wf.ID)]
		if !ok {
			log.Printf("warn: no runs found for workflow %s", w.Name)
			continue
		}
		runs := runsData.WorkflowRuns

		sort.Slice(runs, func(i, j int) bool {
			return runs[i].CreatedAt.After(runs[j].CreatedAt)
		})

		recent := runs
		if len(recent) > recentLimit {
			recent = runs[:recentLimit]
		}

		log.Printf("fetching logs for failed runs in %s…", w.Name)
		for i := range recent {
			client.fetchJobSummaries(&recent[i])
			if recent[i].Conclusion == "failure" {
				client.enrichWithLogs(&recent[i])
			}
		}

		summary := buildSummary(runs, w.Name, w.Description, w.Critical, w.Required)
		summary.RecentRuns = recent
		summaries = append(summaries, summary)
		totalHealth += (100 - summary.FailureRate)
	}

	health := 0.0
	if len(summaries) > 0 {
		health = totalHealth / float64(len(summaries))
	}

	data := DashboardData{
		GeneratedAt:   time.Now().UTC(),
		Repo:          cfg.Settings.SourceRepo,
		OverallHealth: health,
		Workflows:     summaries,
	}

	out, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		log.Fatalf("cannot marshal stats: %v", err)
	}
	if err := os.WriteFile("stats.json", out, 0644); err != nil {
		log.Fatalf("cannot write stats.json: %v", err)
	}
}
