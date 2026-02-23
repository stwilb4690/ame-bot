#!/bin/bash
cd /Volumes/AI_Workspace/projects/ame-bot
export $(grep -v '^#' .env | xargs)
exec ./target/release/ame-bot
