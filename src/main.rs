use anyhow::Result;
use core::f64;
use crossterm::style::{Stylize, style};
use crossterm::{QueueableCommand, cursor, event, style, terminal};
use duckdb::Connection;
use duckdb::arrow::array::ArrowNativeTypeOp;
use std::io::{self, Write};
use std::thread::sleep;
use std::time::{Duration, Instant};

const SETUP_SQL: &str = r#"
-- Game constants: field dimensions and paddle properties
CREATE TEMP TABLE params AS
SELECT
    80 AS W,              -- Width of the playing field (characters)
    25 AS H,              -- Height of the playing field (characters)
    7  AS PADDLE_H,       -- Height of each paddle (characters)
    2  AS PADDLE_SPEED;   -- How fast paddles can move per frame

-- Game state: positions, velocities, and scores
-- This single row gets updated every frame with new positions
CREATE TEMP TABLE state(
    tick    INTEGER,      -- Frame counter (increases each update)
    ax      INTEGER,      -- Player A paddle Y position (left side)
    bx      INTEGER,      -- Player B paddle Y position (right side)
    ball_x  INTEGER,      -- Ball X position (0 to W-1)
    ball_y  INTEGER,      -- Ball Y position (0 to H-1)
    vx      INTEGER,      -- Ball velocity in X direction (±1)
    vy      INTEGER,      -- Ball velocity in Y direction (-2, -1, 0, 1, 2)
    score_a INTEGER,      -- Player A score
    score_b INTEGER       -- Player B score
);

-- Initialize game with random starting position and angle
INSERT INTO state
SELECT
    0,                                                       -- tick = 0 (start)
    (H-PADDLE_H)/2,                                          -- Player A paddle centered
    (H-PADDLE_H)/2,                                          -- Player B paddle centered
    W/2,                                                     -- Ball at horizontal center
    CAST(H/2 + (random() * 6 - 3) AS INTEGER),               -- Ball Y: center ± 3 pixels
    CASE WHEN random() < 0.5 THEN 1 ELSE -1 END,             -- Ball direction: random left/right
    CAST((random() * 5 - 2) AS INTEGER),                     -- Ball angle: -2 to +2 (5 angles)
    0,                                                       -- Score A = 0
    0                                                        -- Score B = 0
FROM params;
"#;

const TICK_SQL: &str = r#"
-- Use CTEs (Common Table Expressions) to break down the game logic into clear steps
-- Each WITH clause is like a mini-table that feeds into the next step
WITH
    -- Load game parameters and current state for easy reference
    p AS (SELECT * FROM params),
    s AS (SELECT * FROM state),

-- STEP 1: AI DECISION - Calculate where each paddle should move
-- The AI mimics human players: track defensively, then make strategic shots when close
ai AS (
    SELECT
        -- PLAYER A (left side) - Decide where to move the paddle
        CASE
        -- When ball is CLOSE (≤5 pixels away) and approaching: attempt trick shots!
        -- Position paddle to hit ball at specific zones for different angles
        WHEN s.vx < 0 AND s.ball_x <= 5 THEN
            CASE
                WHEN random() < 0.25 THEN greatest(s.ball_y - 0, 1)  -- Hit top: steep up (vy=-2)
                WHEN random() < 0.50 THEN greatest(s.ball_y - 1, 1)  -- Hit upper: diagonal up (vy=-1)
                WHEN random() < 0.55 THEN greatest(s.ball_y - 3, 1)  -- Hit center: straight (vy=0) RARE!
                WHEN random() < 0.75 THEN greatest(s.ball_y - 5, 1)  -- Hit lower: diagonal down (vy=+1)
                ELSE greatest(s.ball_y - 6, 1)                       -- Hit bottom: steep down (vy=+2)
            END
        -- When ball is FAR: track defensively (85% accuracy for more scoring opportunities)
        WHEN random() < 0.85 THEN
            CASE
                WHEN s.ball_y < s.ax + 2 THEN greatest(s.ax - p.PADDLE_SPEED, 1)
                WHEN s.ball_y > s.ax + p.PADDLE_H - 3 THEN least(s.ax + p.PADDLE_SPEED, p.H - p.PADDLE_H - 1)
                ELSE s.ax
            END
        -- 15% of the time: don't move (more imperfection for shorter games)
        ELSE s.ax
        END AS ax2,
        -- PLAYER B (right side) - Same logic but mirrored
        -- Can be controlled by human player
        CASE
        WHEN s.vx > 0 AND s.ball_x >= p.W - 6 THEN
            CASE
                WHEN random() < 0.25 THEN greatest(s.ball_y - 0, 1)
                WHEN random() < 0.50 THEN greatest(s.ball_y - 1, 1)
                WHEN random() < 0.55 THEN greatest(s.ball_y - 3, 1)
                WHEN random() < 0.75 THEN greatest(s.ball_y - 5, 1)
                ELSE greatest(s.ball_y - 6, 1)
            END
        WHEN random() < 0.85 THEN
            CASE
                WHEN s.ball_y < s.bx + 2 THEN greatest(s.bx - p.PADDLE_SPEED, 1)
                WHEN s.ball_y > s.bx + p.PADDLE_H - 3 THEN least(s.bx + p.PADDLE_SPEED, p.H - p.PADDLE_H - 1)
                ELSE s.bx
            END
        ELSE s.bx
        END AS bx2
    FROM p, s
),

-- STEP 2: BALL MOVEMENT - Move ball by its velocity
step AS (
    SELECT
        s.ball_x + s.vx AS nx,
        s.ball_y + s.vy AS ny,
        s.vx,
        s.vy
    FROM s
),

-- STEP 3: WALL COLLISION - Bounce ball off top/bottom walls
wall AS (
    SELECT
        nx,
        CASE WHEN ny <= 1 THEN 1 WHEN ny >= p.H-2 THEN p.H-2 ELSE ny END AS ny1,
        vx AS vx1,
        CASE WHEN ny <= 1 OR ny >= p.H-2 THEN -vy ELSE vy END AS vy1  -- Flip Y velocity
    FROM step, p
),

-- STEP 4: PADDLE COLLISION - Detect hits and calculate bounce angles
-- This is the magic! Ball angle depends on WHERE it hits the paddle (classic Pong physics)
paddle AS (
    SELECT
        w.nx, w.ny1,
        -- Reverse horizontal direction if paddle hit
        CASE
            WHEN w.nx <= 1     AND w.vx1 < 0 AND w.ny1 BETWEEN ai.ax2 AND ai.ax2 + p.PADDLE_H - 1 THEN 1
            WHEN w.nx >= p.W-2 AND w.vx1 > 0 AND w.ny1 BETWEEN ai.bx2 AND ai.bx2 + p.PADDLE_H - 1 THEN -1
            ELSE w.vx1
        END AS vx2,
        -- Calculate new vertical velocity based on hit zone (5 zones on paddle)
        -- Top edge = steep up (-2), Center = straight (0), Bottom edge = steep down (+2)
        CASE
            WHEN w.nx <= 1 AND w.vx1 < 0 AND w.ny1 BETWEEN ai.ax2 AND ai.ax2 + p.PADDLE_H - 1 THEN
                CASE
                    WHEN w.ny1 - ai.ax2 =  0 THEN -2     -- Position 0: top edge
                    WHEN w.ny1 - ai.ax2 <= 2 THEN -1     -- Positions 1-2: upper
                    WHEN w.ny1 - ai.ax2 <= 4 THEN 0      -- Positions 3-4: center
                    WHEN w.ny1 - ai.ax2 <= 5 THEN 1      -- Position 5: lower
                    ELSE 2                               -- Position 6: bottom edge
                END
            WHEN w.nx >= p.W-2 AND w.vx1 > 0 AND w.ny1 BETWEEN ai.bx2 AND ai.bx2 + p.PADDLE_H - 1 THEN
                CASE
                    WHEN w.ny1 - ai.bx2 =  0 THEN -2
                    WHEN w.ny1 - ai.bx2 <= 2 THEN -1
                    WHEN w.ny1 - ai.bx2 <= 4 THEN 0
                    WHEN w.ny1 - ai.bx2 <= 5 THEN 1
                    ELSE 2
                END
            ELSE w.vy1
        END AS vy2,
        ai.ax2 AS ax2, ai.bx2 AS bx2
    FROM wall w, ai, p
),

-- STEP 5: SCORING - Detect if ball went past a paddle
sc AS (
    SELECT
        CASE
            WHEN paddle.nx < 1 THEN 'B'              -- Ball past left: Player B scores
            WHEN paddle.nx > p.W-2 THEN 'A'          -- Ball past right: Player A scores
            ELSE NULL                                -- NULL = still in play
        END AS point_to,
        paddle.*, p.W, p.H
    FROM paddle, p
),

-- STEP 6: UPDATE STATE - Combine all changes and increment scores
next_state AS (
    SELECT
        s.tick + 1 AS tick,                           -- Increment frame counter
        sc.ax2 AS ax, sc.bx2 AS bx,                   -- New paddle positions
        -- Ball position: reset to center if scored, otherwise use new position
        CASE
            WHEN sc.point_to IS NULL THEN sc.nx
            WHEN sc.point_to='A' THEN sc.W/2 + 1
            ELSE sc.W/2 - 1
        END AS ball_x,
        CASE
            WHEN sc.point_to IS NULL THEN sc.ny1
            ELSE CAST(sc.H/2 + (random() * 6 - 3) AS INTEGER)
        END AS ball_y,
        -- Ball velocity: keep current if in play, otherwise random serve
        CASE
            WHEN sc.point_to IS NULL THEN sc.vx2
            WHEN sc.point_to='A' THEN -1
            ELSE 1
        END AS vx,
        CASE
            WHEN sc.point_to IS NULL THEN sc.vy2
            ELSE CAST((random() * 5 - 2) AS INTEGER)
        END AS vy,
        -- Increment score if someone scored
        s.score_a + COALESCE((sc.point_to='A')::INT, 0) AS score_a,
        s.score_b + COALESCE((sc.point_to='B')::INT, 0) AS score_b
    FROM sc, state s
)

-- Finally, write the new state back to the state table
UPDATE state
SET tick = n.tick, ax = n.ax, bx = n.bx,
    ball_x = n.ball_x, ball_y = n.ball_y,
    vx = n.vx, vy = n.vy,
    score_a = n.score_a, score_b = n.score_b
FROM next_state n;
"#;

const RENDER_SQL: &str = r#"
-- Generate the entire game screen as ASCII art, one character at a time
-- This creates an 80x25 grid and decides what character to put in each position
SELECT y,
    string_agg(
        CASE
        WHEN y IN (0,p.H-1) THEN '▀'                                         -- Top/bottom borders
        WHEN x=1 AND y BETWEEN s.ax AND s.ax + p.PADDLE_H - 1 THEN '█'       -- Player A paddle (left)
        WHEN x=p.W-2 AND y BETWEEN s.bx AND s.bx + p.PADDLE_H - 1 THEN '█'   -- Player B paddle (right)
        WHEN x=s.ball_x AND y=s.ball_y THEN '█'                              -- Ball
        WHEN x=p.W/2 AND (y % 3)=1 THEN '█'                                  -- Center line (dotted)
        ELSE ' '                                                             -- Empty space
        END, ''
    ) AS line
FROM params p, state s, range(0,p.H) AS t_y(y), range(0,p.W) AS t_x(x)
GROUP BY y
ORDER BY y;
"#;

fn main() -> Result<()> {
    let fps = 120;
    let frame_dt = Duration::from_secs_f64(1.0 / fps as f64);

    let conn = Connection::open_in_memory()?;
    conn.execute(SETUP_SQL, [])?;

    terminal::enable_raw_mode()?;
    let mut stdout = io::BufWriter::new(io::stdout());
    stdout
        .queue(terminal::Clear(terminal::ClearType::All))?
        .queue(cursor::Hide)?
        .flush()?;

    loop {
        if event::poll(Duration::ZERO)? {
            if let event::Event::Key(key_event) = event::read()? {
                if key_event.code == event::KeyCode::Esc {
                    break;
                }
            }
        }

        let frame_start = Instant::now();
        conn.execute(TICK_SQL, [])?;
        let mut stmt = conn.prepare(RENDER_SQL)?;
        let mut rows = stmt.query([])?;

        stdout
            .queue(cursor::MoveTo(0, 0))?
            .queue(terminal::Clear(terminal::ClearType::FromCursorDown))?;
        while let Some(row) = rows.next()? {
            let line = row.get::<&str, String>("line")?;
            stdout
                .queue(cursor::MoveToNextLine(1))?
                .queue(style::Print(line))?;
        }

        let frame_time = frame_start.elapsed();
        let sleep_for = frame_dt.checked_sub(frame_time).unwrap_or(Duration::ZERO);
        let actual_fps = (1.0
            .div_checked((frame_time + sleep_for).as_secs_f64())
            .unwrap_or(0.0)) as i32;

        stdout
            .queue(cursor::MoveToNextLine(1))?
            .queue(style::Print("Press ESC to exit, FPS: "))?
            .queue(style::PrintStyledContent(
                style(actual_fps).with(style::Color::Yellow),
            ))?
            .flush()?;
        sleep(sleep_for);
    }
    stdout.queue(cursor::Show)?.flush()?;
    terminal::disable_raw_mode()?;

    Ok(())
}
