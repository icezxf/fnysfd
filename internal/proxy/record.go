package proxy

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"fnysfd/internal/logger"

	_ "modernc.org/sqlite"
)

// RecordService 飞牛观看记录服务
type RecordService struct {
	srcPath string
	tmpPath string

	logger *logger.Logger

	copyMu     sync.Mutex
	lastCopyAt time.Time
	copyExpire time.Duration

	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewRecordService 创建观看记录服务
func NewRecordService(srcPath string, log *logger.Logger) *RecordService {
	if srcPath == "" {
		return nil
	}
	rs := &RecordService{
		srcPath:    srcPath,
		tmpPath:    filepath.Join(os.TempDir(), "fnysfd_trimmedia_tmp.db"),
		logger:     log,
		copyExpire: 60 * time.Second,
		stopCh:     make(chan struct{}),
	}
	if _, err := os.Stat(srcPath); err != nil {
		log.Warn("🎬 [观看记录] 源数据库不可访问: %s (%v)", srcPath, err)
	}
	return rs
}

// Stop 停止
func (rs *RecordService) Stop() {
	if rs == nil {
		return
	}
	rs.stopOnce.Do(func() { close(rs.stopCh) })
}

// Available 判断服务是否可用
func (rs *RecordService) Available() bool {
	if rs == nil {
		return false
	}
	_, err := os.Stat(rs.srcPath)
	return err == nil
}

// ============================================================
// 数据库副本管理
// ============================================================

func (rs *RecordService) ensureCopy() error {
	rs.copyMu.Lock()
	defer rs.copyMu.Unlock()

	if !rs.lastCopyAt.IsZero() && time.Since(rs.lastCopyAt) < rs.copyExpire {
		if _, err := os.Stat(rs.tmpPath); err == nil {
			return nil
		}
	}
	return rs.atomicCopy()
}

func (rs *RecordService) atomicCopy() error {
	src, err := os.Open(rs.srcPath)
	if err != nil {
		return fmt.Errorf("打开源数据库失败: %w", err)
	}
	defer src.Close()

	if err := os.MkdirAll(filepath.Dir(rs.tmpPath), 0755); err != nil {
		return fmt.Errorf("创建临时目录失败: %w", err)
	}

	tmpNew := rs.tmpPath + ".new"
	dst, err := os.Create(tmpNew)
	if err != nil {
		return fmt.Errorf("创建临时文件失败: %w", err)
	}

	if _, err := io.Copy(dst, src); err != nil {
		dst.Close()
		os.Remove(tmpNew)
		return fmt.Errorf("拷贝数据库失败: %w", err)
	}
	if err := dst.Close(); err != nil {
		os.Remove(tmpNew)
		return fmt.Errorf("关闭临时文件失败: %w", err)
	}

	if err := os.Rename(tmpNew, rs.tmpPath); err != nil {
		os.Remove(tmpNew)
		return fmt.Errorf("重命名失败: %w", err)
	}

	rs.lastCopyAt = time.Now()
	rs.logger.Debug("🎬 [观看记录] 数据库副本已更新: %s", rs.tmpPath)
	return nil
}

func (rs *RecordService) openDB() (*sql.DB, error) {
	if err := rs.ensureCopy(); err != nil {
		return nil, err
	}
	dsn := fmt.Sprintf("file:%s?mode=ro&_pragma=busy_timeout(5000)", rs.tmpPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

// ============================================================
// 数据类型
// ============================================================

type PlayStats struct {
	TotalUsers  int    `json:"total_users"`
	TotalPlays  int    `json:"total_plays"`
	ActiveUsers int    `json:"active_users"`
	TodayPlays  int    `json:"today_plays"`
	LatestPlay  string `json:"latest_play"`
}

type RecordUser struct {
	GUID      string `json:"guid"`
	Username  string `json:"username"`
	LastLogin string `json:"last_login"`
	IsAdmin   int    `json:"is_admin"`
	Status    int    `json:"status"`
}

type PlayRecord struct {
	ItemGUID          string                   `json:"item_guid"`
	UserGUID          string                   `json:"user_guid"`
	Username          string                   `json:"username"`
	Title             string                   `json:"title"`
	OriginalTitle     string                   `json:"original_title"`
	Overview          string                   `json:"overview"`
	Type              string                   `json:"type"`
	PlayType          string                   `json:"play_type"`
	SeasonNumber      *int                     `json:"season_number"`
	EpisodeNumber     *int                     `json:"episode_number"`
	SeriesTitle       string                   `json:"series_title"`
	Hierarchy         []map[string]interface{} `json:"hierarchy"`
	Position          int64                    `json:"position"`
	PositionFormatted string                   `json:"position_formatted"`
	Runtime           int64                    `json:"runtime"`
	RuntimeFormatted  string                   `json:"runtime_formatted"`
	Progress          float64                  `json:"progress"`
	Watched           bool                     `json:"watched"`
	Resolution        string                   `json:"resolution"`
	CreateTime        string                   `json:"create_time"`
	UpdateTime        string                   `json:"update_time"`
	ReleaseDate       string                   `json:"release_date"`
	IsEpisode         bool                     `json:"is_episode"`
}

type HistoryResult struct {
	Total   int          `json:"total"`
	Page    int          `json:"page"`
	PerPage int          `json:"per_page"`
	Pages   int          `json:"pages"`
	Data    []PlayRecord `json:"data"`
}

type HistoryQuery struct {
	UserGUID    string
	Page        int
	PerPage     int
	SearchTitle string
	StartTime   string
	EndTime     string
}

// ============================================================
// 查询：统计数据
// ============================================================

func (rs *RecordService) GetStats() (*PlayStats, error) {
	db, err := rs.openDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	stats := &PlayStats{}

	if err := db.QueryRow(`SELECT COUNT(*) FROM user WHERE status = 1 AND guid != 'default-user-template'`).Scan(&stats.TotalUsers); err != nil {
		return nil, err
	}

	if err := db.QueryRow(`
		SELECT COUNT(*) FROM item_user_play iup
		JOIN user u ON iup.user_guid = u.guid
		JOIN item i ON iup.item_guid = i.guid
		WHERE iup.visible = 1
	`).Scan(&stats.TotalPlays); err != nil {
		return nil, err
	}

	if err := db.QueryRow(`SELECT COUNT(DISTINCT user_guid) FROM item_user_play WHERE visible = 1`).Scan(&stats.ActiveUsers); err != nil {
		return nil, err
	}

	now := time.Now()
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()).UnixMilli()
	if err := db.QueryRow(`
		SELECT COUNT(*) FROM item_user_play iup
		JOIN user u ON iup.user_guid = u.guid
		JOIN item i ON iup.item_guid = i.guid
		WHERE iup.visible = 1 AND iup.update_time >= ?
	`, todayStart).Scan(&stats.TodayPlays); err != nil {
		return nil, err
	}

	var latest sql.NullInt64
	if err := db.QueryRow(`SELECT MAX(update_time) FROM item_user_play WHERE visible = 1`).Scan(&latest); err == nil && latest.Valid {
		stats.LatestPlay = formatTimestamp(latest.Int64)
	}

	return stats, nil
}

// ============================================================
// 查询：用户列表
// ============================================================

func (rs *RecordService) GetUsers() ([]RecordUser, error) {
	db, err := rs.openDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query(`
		SELECT guid, username, last_login_time, is_admin, status
		FROM user
		WHERE status = 1 AND guid != 'default-user-template'
		ORDER BY username
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []RecordUser
	for rows.Next() {
		var u RecordUser
		var lastLogin sql.NullInt64
		if err := rows.Scan(&u.GUID, &u.Username, &lastLogin, &u.IsAdmin, &u.Status); err != nil {
			return nil, err
		}
		if lastLogin.Valid {
			u.LastLogin = formatTimestamp(lastLogin.Int64)
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

// ============================================================
// 查询：播放历史（分页 + 筛选）
// ============================================================

func (rs *RecordService) GetHistory(q HistoryQuery) (*HistoryResult, error) {
	if q.Page < 1 {
		q.Page = 1
	}
	if q.PerPage < 1 || q.PerPage > 200 {
		q.PerPage = 20
	}

	db, err := rs.openDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	whereClause := "WHERE iup.visible = 1"
	var params []interface{}

	if q.UserGUID != "" {
		whereClause += " AND iup.user_guid = ?"
		params = append(params, q.UserGUID)
	}

	if q.SearchTitle != "" {
		whereClause += `
			AND EXISTS (
				WITH RECURSIVE item_hierarchy(guid, title, original_title, parent_guid, level) AS (
					SELECT guid, title, original_title, parent_guid, 0 as level
					FROM item WHERE guid = i.guid
					UNION ALL
					SELECT parent.guid, parent.title, parent.original_title, parent.parent_guid, ih.level + 1
					FROM item parent
					INNER JOIN item_hierarchy ih ON parent.guid = ih.parent_guid
					WHERE ih.level < 10 AND parent.guid IS NOT NULL
				)
				SELECT 1 FROM item_hierarchy WHERE title LIKE ? OR original_title LIKE ?
			)
		`
		sp := "%" + q.SearchTitle + "%"
		params = append(params, sp, sp)
	}

	if q.StartTime != "" {
		if ts, ok := parseLocalTime(q.StartTime); ok {
			whereClause += " AND iup.update_time >= ?"
			params = append(params, ts)
		}
	}
	if q.EndTime != "" {
		if ts, ok := parseLocalTime(q.EndTime); ok {
			whereClause += " AND iup.update_time <= ?"
			params = append(params, ts)
		}
	}

	var total int
	countSQL := `
		SELECT COUNT(*) FROM item_user_play iup
		JOIN user u ON iup.user_guid = u.guid
		JOIN item i ON iup.item_guid = i.guid
	` + whereClause
	if err := db.QueryRow(countSQL, params...).Scan(&total); err != nil {
		return nil, err
	}

	if total == 0 {
		return &HistoryResult{Total: 0, Page: q.Page, PerPage: q.PerPage, Pages: 0, Data: []PlayRecord{}}, nil
	}

	offset := (q.Page - 1) * q.PerPage
	listSQL := `
		SELECT
			iup.item_guid, iup.user_guid, iup.ts as position, iup.watched,
			iup.create_time, iup.update_time, iup.type as play_type, iup.resolution,
			u.username,
			i.title, i.original_title, i.overview, i.type as item_type,
			i.season_number, i.episode_number, i.parent_guid, i.runtime, i.release_date
		FROM item_user_play iup
		JOIN user u ON iup.user_guid = u.guid
		JOIN item i ON iup.item_guid = i.guid
	` + whereClause + `
		ORDER BY iup.update_time DESC
		LIMIT ? OFFSET ?
	`
	listParams := append([]interface{}{}, params...)
	listParams = append(listParams, q.PerPage, offset)

	rows, err := db.Query(listSQL, listParams...)
	if err != nil {
		return nil, err
	}

	// ✅ 先把所有行读完，立刻关闭 rows，释放连接
	//    否则后续 getItemHierarchy 的 Query 拿不到连接会死锁
	var rawRecords []PlayRecord
	for rows.Next() {
		var rec PlayRecord
		var position, watched, createTime, updateTime sql.NullInt64
		var runtime, seasonNum, episodeNum sql.NullInt64
		var resolution, originalTitle, overview, itemType, releaseDate sql.NullString

		if err := rows.Scan(
			&rec.ItemGUID, &rec.UserGUID, &position, &watched,
			&createTime, &updateTime, &rec.PlayType, &resolution,
			&rec.Username,
			&rec.Title, &originalTitle, &overview, &itemType,
			&seasonNum, &episodeNum, &rec.UserGUID, &runtime, &releaseDate,
		); err != nil {
			rows.Close()
			return nil, err
		}

		rec.Position = position.Int64
		rec.Watched = watched.Int64 != 0
		rec.Runtime = runtime.Int64 * 60
		rec.OriginalTitle = originalTitle.String
		rec.Overview = overview.String
		rec.Type = itemType.String
		rec.Resolution = resolution.String
		rec.ReleaseDate = releaseDate.String
		rec.CreateTime = formatTimestamp(createTime.Int64)
		rec.UpdateTime = formatTimestamp(updateTime.Int64)

		if seasonNum.Valid {
			v := int(seasonNum.Int64)
			rec.SeasonNumber = &v
		}
		if episodeNum.Valid {
			v := int(episodeNum.Int64)
			rec.EpisodeNumber = &v
		}
		rec.IsEpisode = rec.SeasonNumber != nil && rec.EpisodeNumber != nil

		rawRecords = append(rawRecords, rec)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close() // ✅ 显式关闭，后面才能安全发新查询

	// ✅ rows 关掉之后，再逐条补 hierarchy（此时连接池空闲）
	hierCache := make(map[string][]map[string]interface{})
	records := make([]PlayRecord, 0, len(rawRecords))
	for _, rec := range rawRecords {
		hierarchy := rs.getItemHierarchy(db, rec.ItemGUID, hierCache)
		rec.Hierarchy = hierarchy

		displayTitle := rec.Title
		seriesInfo := ""
		if len(hierarchy) > 1 {
			rootItem := hierarchy[len(hierarchy)-1]
			if t, ok := rootItem["title"].(string); ok {
				seriesInfo = t
			}
			if rec.SeasonNumber != nil && rec.EpisodeNumber != nil {
				displayTitle = fmt.Sprintf("%s - S%02dE%02d - %s", seriesInfo, *rec.SeasonNumber, *rec.EpisodeNumber, rec.Title)
			} else if rec.Title != seriesInfo {
				displayTitle = fmt.Sprintf("%s - %s", seriesInfo, rec.Title)
			}
		} else if rec.SeasonNumber != nil && rec.EpisodeNumber != nil {
			displayTitle = fmt.Sprintf("S%02dE%02d - %s", *rec.SeasonNumber, *rec.EpisodeNumber, rec.Title)
		}
		rec.Title = displayTitle
		rec.SeriesTitle = seriesInfo

		if rec.Runtime > 0 && rec.Position > 0 {
			p := float64(rec.Position) / float64(rec.Runtime) * 100
			if p > 100 {
				p = 100
			}
			rec.Progress = float64(int(p*10)) / 10
		}
		rec.PositionFormatted = formatDuration(rec.Position)
		rec.RuntimeFormatted = formatDuration(rec.Runtime)

		records = append(records, rec)
	}

	pages := (total + q.PerPage - 1) / q.PerPage
	return &HistoryResult{
		Total:   total,
		Page:    q.Page,
		PerPage: q.PerPage,
		Pages:   pages,
		Data:    records,
	}, nil
}

// getItemHierarchy 递归查询父级链
func (rs *RecordService) getItemHierarchy(db *sql.DB, itemGUID string, cache map[string][]map[string]interface{}) []map[string]interface{} {
	if cached, ok := cache[itemGUID]; ok {
		return cached
	}

	rows, err := db.Query(`
		WITH RECURSIVE item_hierarchy(guid, title, original_title, parent_guid, level) AS (
			SELECT guid, title, original_title, parent_guid, 0 as level
			FROM item WHERE guid = ?
			UNION ALL
			SELECT i.guid, i.title, i.original_title, i.parent_guid, ih.level + 1
			FROM item i
			INNER JOIN item_hierarchy ih ON i.guid = ih.parent_guid
			WHERE ih.level < 10 AND i.guid IS NOT NULL
		)
		SELECT guid, title, original_title, parent_guid, level
		FROM item_hierarchy ORDER BY level ASC
	`, itemGUID)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var hierarchy []map[string]interface{}
	for rows.Next() {
		var guid, title, parentGUID sql.NullString
		var originalTitle sql.NullString
		var level int
		if err := rows.Scan(&guid, &title, &originalTitle, &parentGUID, &level); err != nil {
			continue
		}
		hierarchy = append(hierarchy, map[string]interface{}{
			"guid":           guid.String,
			"title":          title.String,
			"original_title": originalTitle.String,
			"parent_guid":    parentGUID.String,
			"level":          level,
		})
	}
	cache[itemGUID] = hierarchy
	return hierarchy
}

// ============================================================
// 工具函数
// ============================================================

func formatTimestamp(ms int64) string {
	if ms <= 0 {
		return ""
	}
	return time.UnixMilli(ms).Format("2006-01-02 15:04:05")
}

func formatDuration(seconds int64) string {
	if seconds <= 0 {
		return "00:00:00"
	}
	h := seconds / 3600
	m := (seconds % 3600) / 60
	s := seconds % 60
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

func parseLocalTime(s string) (int64, bool) {
	t, err := time.ParseInLocation("2006-01-02 15:04:05", strings.TrimSpace(s), time.Local)
	if err != nil {
		return 0, false
	}
	return t.UnixMilli(), true
}
