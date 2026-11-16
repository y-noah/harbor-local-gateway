// Apache-2.0; derived from OpenAI Codex rust-v0.167.2 protocol ModelInfo types.
// Source/license: third-party/codex-models/source.json and LICENSE.txt.
// Unknown JSON fields are preserved; known fields follow the pinned CLI deserializer.
export const modelProfileContract={
  "ApprovalMessages": {
    "on_request": {
      "type": "Option<String>",
      "required": false
    },
    "on_request_auto_review": {
      "type": "Option<String>",
      "required": false
    },
    "never": {
      "type": "Option<String>",
      "required": false
    },
    "unless_trusted": {
      "type": "Option<String>",
      "required": false
    },
    "request_permissions": {
      "type": "Option<String>",
      "required": false
    },
    "request_permissions_tool": {
      "type": "Option<String>",
      "required": false,
      "aliasFor": "request_permissions"
    },
    "auto_review_suffix": {
      "type": "Option<String>",
      "required": false
    },
    "granular": {
      "type": "Option<GranularApprovalMessages>",
      "required": false
    }
  },
  "AppsMessages": {
    "usage_instructions": {
      "type": "Option<String>",
      "required": false
    }
  },
  "AutoReviewMessages": {
    "policy": {
      "type": "Option<String>",
      "required": false
    },
    "policy_template": {
      "type": "Option<String>",
      "required": false
    },
    "node_repl_policy": {
      "type": "Option<String>",
      "required": false
    },
    "rejection_instructions": {
      "type": "Option<String>",
      "required": false
    },
    "timeout_instructions": {
      "type": "Option<String>",
      "required": false
    }
  },
  "CodeModeToolMessages": {
    "exec": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "wait": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "deferred_nested_tools_guidance": {
      "type": "Option<String>",
      "required": false
    },
    "mcp_typescript_preamble": {
      "type": "Option<String>",
      "required": false
    }
  },
  "CollaborationModeMessages": {
    "default": {
      "type": "Option<String>",
      "required": false
    },
    "plan": {
      "type": "Option<String>",
      "required": false
    }
  },
  "ConfirmationPolicies": {
    "browser_use": {
      "type": "Option<String>",
      "required": false
    },
    "computer_use": {
      "type": "Option<String>",
      "required": false
    }
  },
  "ContextManagementToolMessages": {
    "new_context": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "get_context_remaining": {
      "type": "Option<ToolMessage>",
      "required": false
    }
  },
  "GranularApprovalMessages": {
    "intro": {
      "type": "Option<String>",
      "required": false
    },
    "prompted_categories": {
      "type": "Option<String>",
      "required": false
    },
    "rejected_categories": {
      "type": "Option<String>",
      "required": false
    }
  },
  "GuardianModelPolicy": {
    "computer_use": {
      "type": "Option<GuardianReviewMode>",
      "required": false
    },
    "shell": {
      "type": "Option<GuardianReviewMode>",
      "required": false
    },
    "file_changes": {
      "type": "Option<GuardianReviewMode>",
      "required": false
    },
    "mcp": {
      "type": "Option<GuardianReviewMode>",
      "required": false
    },
    "network": {
      "type": "Option<GuardianReviewMode>",
      "required": false
    },
    "permissions": {
      "type": "Option<GuardianReviewMode>",
      "required": false
    },
    "other_tools": {
      "type": "GuardianReviewMode",
      "required": false
    },
    "unscored_action": {
      "type": "GuardianUnscoredAction",
      "required": false
    },
    "initial_cua_call": {
      "type": "Option<bool>",
      "required": false
    },
    "sandboxed_exec_commands": {
      "type": "Option<bool>",
      "required": false
    }
  },
  "GuardianV2ModelConfig": {
    "async_classifier_mode": {
      "type": "Option<AsyncClassifierMode>",
      "required": false
    },
    "async_classifier_conversation_token_limit": {
      "type": "Option<usize>",
      "required": false
    },
    "classifier_instructions": {
      "type": "Option<String>",
      "required": false
    },
    "review_threshold_basis_points": {
      "type": "Option<u16>",
      "required": false
    },
    "max_tool_call_lag": {
      "type": "Option<usize>",
      "required": false
    },
    "reasoning_effort": {
      "type": "Option<ReasoningEffort>",
      "required": false
    },
    "transcript": {
      "type": "Option<GuardianV2TranscriptModelConfig>",
      "required": false
    },
    "max_action_tokens": {
      "type": "Option<usize>",
      "required": false
    },
    "max_classifier_instruction_tokens": {
      "type": "Option<usize>",
      "required": false
    },
    "reuse_parent_compaction": {
      "type": "Option<bool>",
      "required": false
    },
    "max_parent_compaction_tokens": {
      "type": "Option<usize>",
      "required": false
    }
  },
  "GuardianV2TranscriptModelConfig": {
    "sources": {
      "type": "Option<Vec<String>>",
      "required": false
    },
    "include_images": {
      "type": "Option<bool>",
      "required": false
    },
    "max_message_entry_tokens": {
      "type": "Option<usize>",
      "required": false
    },
    "max_tool_entry_tokens": {
      "type": "Option<usize>",
      "required": false
    },
    "max_message_transcript_tokens": {
      "type": "Option<usize>",
      "required": false
    },
    "max_tool_transcript_tokens": {
      "type": "Option<usize>",
      "required": false
    },
    "max_recent_non_user_entries": {
      "type": "Option<usize>",
      "required": false
    }
  },
  "HistoryToolMessages": {
    "namespace_description": {
      "type": "Option<String>",
      "required": false
    },
    "list_windows": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "list_items": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "read_item": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "search_contents": {
      "type": "Option<ToolMessage>",
      "required": false
    }
  },
  "IncrementalToolMessages": {
    "tool_update_hint": {
      "type": "Option<String>",
      "required": false
    },
    "removed_tools_header": {
      "type": "Option<String>",
      "required": false
    },
    "removed_namespaces_header": {
      "type": "Option<String>",
      "required": false
    },
    "namespace_instructions_prefix": {
      "type": "Option<String>",
      "required": false
    },
    "namespace_instructions_cleared": {
      "type": "Option<String>",
      "required": false
    }
  },
  "IndirectDescriptionPrefixes": {
    "namespaces": {
      "type": "Option<std::collections::BTreeMap<String,String>>",
      "required": false
    },
    "mcp_servers": {
      "type": "Option<std::collections::BTreeMap<String,String>>",
      "required": false
    }
  },
  "McpResourceToolMessages": {
    "list_mcp_resources": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "list_mcp_resource_templates": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "read_mcp_resource": {
      "type": "Option<ToolMessage>",
      "required": false
    }
  },
  "ModelAccessPrograms": {
    "cyber": {
      "type": "Vec<CyberAccessProgram>",
      "required": true
    }
  },
  "ModelAvailabilityNux": {
    "message": {
      "type": "String",
      "required": true
    }
  },
  "ModelInfo": {
    "guardian": {
      "type": "Option<GuardianModelPolicy>",
      "required": false
    },
    "slug": {
      "type": "String",
      "required": true
    },
    "display_name": {
      "type": "String",
      "required": true
    },
    "description": {
      "type": "Option<String>",
      "required": false
    },
    "default_reasoning_level": {
      "type": "Option<ReasoningEffort>",
      "required": false
    },
    "supported_reasoning_levels": {
      "type": "Vec<ReasoningEffortPreset>",
      "required": true
    },
    "shell_type": {
      "type": "ConfigShellToolType",
      "required": true
    },
    "visibility": {
      "type": "ModelVisibility",
      "required": true
    },
    "supported_in_api": {
      "type": "bool",
      "required": true
    },
    "priority": {
      "type": "i32",
      "required": true
    },
    "additional_speed_tiers": {
      "type": "Vec<String>",
      "required": false
    },
    "service_tiers": {
      "type": "Vec<ModelServiceTier>",
      "required": false
    },
    "default_service_tier": {
      "type": "Option<String>",
      "required": false
    },
    "available_access_programs": {
      "type": "Option<ModelAccessPrograms>",
      "required": false
    },
    "availability_nux": {
      "type": "Option<ModelAvailabilityNux>",
      "required": false
    },
    "upgrade": {
      "type": "Option<ModelInfoUpgrade>",
      "required": false
    },
    "model_messages": {
      "type": "Option<ModelMessages>",
      "required": false
    },
    "include_skills_usage_instructions": {
      "type": "bool",
      "required": false
    },
    "include_plugin_usage_instructions": {
      "type": "bool",
      "required": false
    },
    "include_apps_usage_instructions": {
      "type": "bool",
      "required": false
    },
    "supports_reasoning_summary_parameter": {
      "type": "bool",
      "required": false
    },
    "default_reasoning_summary": {
      "type": "ReasoningSummary",
      "required": false
    },
    "support_verbosity": {
      "type": "bool",
      "required": true
    },
    "default_verbosity": {
      "type": "Option<Verbosity>",
      "required": false
    },
    "apply_patch_tool_type": {
      "type": "Option<ApplyPatchToolType>",
      "required": false
    },
    "web_search_tool_type": {
      "type": "WebSearchToolType",
      "required": false
    },
    "truncation_policy": {
      "type": "TruncationPolicyConfig",
      "required": true
    },
    "supports_image_detail_original": {
      "type": "bool",
      "required": false
    },
    "context_window": {
      "type": "Option<i64>",
      "required": false
    },
    "max_context_window": {
      "type": "Option<i64>",
      "required": false
    },
    "auto_compact_token_limit": {
      "type": "Option<i64>",
      "required": false
    },
    "comp_hash": {
      "type": "Option<String>",
      "required": false
    },
    "effective_context_window_percent": {
      "type": "i64",
      "required": false
    },
    "experimental_supported_tools": {
      "type": "Vec<String>",
      "required": true
    },
    "input_modalities": {
      "type": "Vec<InputModality>",
      "required": false
    },
    "supports_search_tool": {
      "type": "bool",
      "required": false
    },
    "supports_experimental_context": {
      "type": "bool",
      "required": false
    },
    "use_responses_lite": {
      "type": "bool",
      "required": false
    },
    "supports_reasoning_effort_updates": {
      "type": "bool",
      "required": false
    },
    "node_repl_auto_review_required": {
      "type": "bool",
      "required": false
    },
    "node_repl_disabled": {
      "type": "bool",
      "required": false
    },
    "auto_review_model_override": {
      "type": "Option<String>",
      "required": false
    },
    "model_specialty": {
      "type": "Option<String>",
      "required": false
    },
    "tool_mode": {
      "type": "Option<ToolMode>",
      "required": false
    },
    "multi_agent_version": {
      "type": "Option<MultiAgentVersion>",
      "required": false
    },
    "multi_agent_reasoning_effort": {
      "type": "Option<ReasoningEffort>",
      "required": false
    },
    "base_instructions": {
      "type": "Option<String>",
      "required": false
    }
  },
  "ModelInfoUpgrade": {
    "model": {
      "type": "String",
      "required": true
    },
    "migration_markdown": {
      "type": "String",
      "required": true
    },
    "retirement_at": {
      "type": "Option<DateTime<Utc>>",
      "required": false
    }
  },
  "ModelMessages": {
    "apps": {
      "type": "Option<AppsMessages>",
      "required": false
    },
    "content_filter_guidance": {
      "type": "Option<String>",
      "required": false
    },
    "persistent_instructions": {
      "type": "Option<String>",
      "required": false
    },
    "tools": {
      "type": "Option<ToolMessages>",
      "required": false
    },
    "instructions_template": {
      "type": "Option<String>",
      "required": false
    },
    "approvals": {
      "type": "Option<ApprovalMessages>",
      "required": false
    },
    "collaboration_modes": {
      "type": "Option<CollaborationModeMessages>",
      "required": false
    },
    "auto_review": {
      "type": "Option<AutoReviewMessages>",
      "required": false
    },
    "permissions": {
      "type": "Option<PermissionMessages>",
      "required": false
    },
    "multi_agent": {
      "type": "Option<MultiAgentMessages>",
      "required": false
    },
    "token_budget": {
      "type": "Option<ModelTokenBudgetConfig>",
      "required": false
    },
    "guardian_v2": {
      "type": "Option<GuardianV2ModelConfig>",
      "required": false
    },
    "confirmation_policies": {
      "type": "Option<ConfirmationPolicies>",
      "required": false
    }
  },
  "ModelServiceTier": {
    "id": {
      "type": "String",
      "required": true
    },
    "name": {
      "type": "String",
      "required": true
    },
    "description": {
      "type": "String",
      "required": true
    }
  },
  "ModelTokenBudgetConfig": {
    "enabled": {
      "type": "bool",
      "required": false
    },
    "use_history_notes_extension": {
      "type": "bool",
      "required": false
    },
    "reminder_threshold_tokens": {
      "type": "i64",
      "required": true
    },
    "reminder_message_template": {
      "type": "String",
      "required": true
    },
    "guidance_message": {
      "type": "String",
      "required": true
    },
    "auto_compact_fallback_prompt": {
      "type": "String",
      "required": true
    },
    "auto_compact_fallback_buffer_tokens": {
      "type": "i64",
      "required": true
    }
  },
  "MultiAgentMessages": {
    "role": {
      "type": "Option<MultiAgentRoleMessages>",
      "required": false
    },
    "mode": {
      "type": "Option<MultiAgentModeMessages>",
      "required": false
    }
  },
  "MultiAgentModeMessages": {
    "explicit": {
      "type": "Option<String>",
      "required": false
    },
    "proactive": {
      "type": "Option<String>",
      "required": false
    },
    "hint_text": {
      "type": "Option<String>",
      "required": false
    }
  },
  "MultiAgentRoleMessages": {
    "root": {
      "type": "Option<String>",
      "required": false
    },
    "subagent": {
      "type": "Option<String>",
      "required": false
    }
  },
  "MultiAgentToolMessages": {
    "spawn_agent": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "send_message": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "followup_task": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "wait_agent": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "interrupt_agent": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "list_agents": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "create_channel": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "get_channels": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "list_threads": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "search_posts": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "read_thread": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "read_post": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "subscribe": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "unsubscribe": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "post": {
      "type": "Option<ToolMessage>",
      "required": false
    }
  },
  "NotesToolMessages": {
    "namespace_description": {
      "type": "Option<String>",
      "required": false
    },
    "list_files_by_prefix": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "read_file": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "search_contents": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "append_to_file": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "write_file": {
      "type": "Option<ToolMessage>",
      "required": false
    }
  },
  "PermissionMessages": {
    "danger_full_access": {
      "type": "Option<String>",
      "required": false
    },
    "workspace_write": {
      "type": "Option<String>",
      "required": false
    },
    "read_only": {
      "type": "Option<String>",
      "required": false
    },
    "approved_prefixes": {
      "type": "Option<String>",
      "required": false
    },
    "approved_command_prefix_saved": {
      "type": "Option<String>",
      "required": false
    },
    "writable_root": {
      "type": "Option<String>",
      "required": false
    },
    "writable_roots": {
      "type": "Option<String>",
      "required": false
    },
    "omitted_paths": {
      "type": "Option<String>",
      "required": false
    },
    "denied_reads": {
      "type": "Option<String>",
      "required": false
    }
  },
  "ReasoningEffortPreset": {
    "effort": {
      "type": "ReasoningEffort",
      "required": true
    },
    "description": {
      "type": "String",
      "required": true
    }
  },
  "ToolMessage": {
    "description": {
      "type": "Option<String>",
      "required": false
    },
    "parameters": {
      "type": "Option<String>",
      "required": false
    }
  },
  "ToolMessages": {
    "history": {
      "type": "Option<HistoryToolMessages>",
      "required": false
    },
    "notes": {
      "type": "Option<NotesToolMessages>",
      "required": false
    },
    "context_management": {
      "type": "Option<ContextManagementToolMessages>",
      "required": false
    },
    "incremental_tools": {
      "type": "Option<IncrementalToolMessages>",
      "required": false
    },
    "functions_namespace_functions_description_prefixes": {
      "type": "Option<std::collections::BTreeMap<String,String>>",
      "required": false
    },
    "indirect_description_prefixes": {
      "type": "Option<IndirectDescriptionPrefixes>",
      "required": false
    },
    "send_user_message_async": {
      "type": "Option<ToolMessage>",
      "required": false
    },
    "multi_agent": {
      "type": "Option<MultiAgentToolMessages>",
      "required": false
    },
    "code_mode": {
      "type": "Option<CodeModeToolMessages>",
      "required": false
    },
    "mcp_resources": {
      "type": "Option<McpResourceToolMessages>",
      "required": false
    }
  },
  "TruncationPolicyConfig": {
    "mode": {
      "type": "TruncationMode",
      "required": true
    },
    "limit": {
      "type": "i64",
      "required": true
    }
  }
};
