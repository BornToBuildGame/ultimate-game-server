-- Migration: 0011_group_edge_indexes
-- Description: group_edge lookup indexes (ADR-0022 Phase 2).

CREATE INDEX IF NOT EXISTS idx_group_edge_source_lookup
    ON group_edge (source_id, state, position DESC);

CREATE INDEX IF NOT EXISTS idx_group_edge_dest_lookup
    ON group_edge (destination_id, state);
