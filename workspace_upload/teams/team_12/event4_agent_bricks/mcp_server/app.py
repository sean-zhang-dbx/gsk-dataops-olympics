"""
MCP Server for Clinical Operations — DataOps Olympics Event 4 Bonus

A FastAPI application exposing clinical action tools via the MCP protocol.
Deploy as a Databricks App and connect via a UC HTTP Connection.

Tools:
  - approve_referral: Approve a specialist referral for a patient
  - schedule_followup: Schedule a follow-up appointment
"""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from datetime import datetime, timedelta
import json
import uuid

app = FastAPI(title="Clinical Actions MCP Server", version="1.0.0")

REFERRAL_LOG: list[dict] = []
FOLLOWUP_LOG: list[dict] = []

MCP_TOOLS = [
    {
        "name": "approve_referral",
        "description": (
            "Approve a specialist referral for a patient. "
            "Requires patient_id, specialist_type (e.g. 'cardiology', 'pulmonology'), "
            "and urgency ('routine', 'urgent', 'emergent')."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "patient_id": {"type": "string", "description": "The patient identifier"},
                "specialist_type": {
                    "type": "string",
                    "description": "Type of specialist (cardiology, pulmonology, etc.)",
                },
                "urgency": {
                    "type": "string",
                    "enum": ["routine", "urgent", "emergent"],
                    "description": "Urgency level of the referral",
                },
                "reason": {"type": "string", "description": "Clinical reason for referral"},
            },
            "required": ["patient_id", "specialist_type", "urgency"],
        },
    },
    {
        "name": "schedule_followup",
        "description": (
            "Schedule a follow-up appointment for a patient. "
            "Requires patient_id, days_from_now, and appointment_type."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "patient_id": {"type": "string", "description": "The patient identifier"},
                "days_from_now": {
                    "type": "integer",
                    "description": "Number of days from today to schedule",
                },
                "appointment_type": {
                    "type": "string",
                    "description": "Type of appointment (in-person, telehealth, lab-only)",
                },
                "notes": {"type": "string", "description": "Additional notes for the appointment"},
            },
            "required": ["patient_id", "days_from_now", "appointment_type"],
        },
    },
]


def _handle_approve_referral(args: dict) -> str:
    ref_id = f"REF-{uuid.uuid4().hex[:8].upper()}"
    record = {
        "referral_id": ref_id,
        "patient_id": args["patient_id"],
        "specialist_type": args["specialist_type"],
        "urgency": args["urgency"],
        "reason": args.get("reason", "Not specified"),
        "status": "APPROVED",
        "approved_at": datetime.now().isoformat(),
    }
    REFERRAL_LOG.append(record)
    return json.dumps({
        "status": "approved",
        "referral_id": ref_id,
        "message": (
            f"Referral {ref_id} approved for patient {args['patient_id']} "
            f"to {args['specialist_type']} ({args['urgency']}). "
            f"Patient will be contacted within "
            f"{'1 hour' if args['urgency'] == 'emergent' else '24 hours' if args['urgency'] == 'urgent' else '5 business days'}."
        ),
    })


def _handle_schedule_followup(args: dict) -> str:
    appt_id = f"APPT-{uuid.uuid4().hex[:8].upper()}"
    scheduled_date = (datetime.now() + timedelta(days=args["days_from_now"])).strftime("%Y-%m-%d")
    record = {
        "appointment_id": appt_id,
        "patient_id": args["patient_id"],
        "scheduled_date": scheduled_date,
        "appointment_type": args["appointment_type"],
        "notes": args.get("notes", ""),
        "created_at": datetime.now().isoformat(),
    }
    FOLLOWUP_LOG.append(record)
    return json.dumps({
        "status": "scheduled",
        "appointment_id": appt_id,
        "scheduled_date": scheduled_date,
        "message": (
            f"Follow-up {appt_id} scheduled for patient {args['patient_id']} "
            f"on {scheduled_date} ({args['appointment_type']})."
        ),
    })


TOOL_HANDLERS = {
    "approve_referral": _handle_approve_referral,
    "schedule_followup": _handle_schedule_followup,
}


@app.post("/mcp")
async def mcp_endpoint(request: Request):
    """
    MCP-compatible endpoint handling JSON-RPC requests.
    Supports: tools/list, tools/call
    """
    body = await request.json()
    method = body.get("method", "")
    req_id = body.get("id", 1)

    if method == "tools/list":
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {"tools": MCP_TOOLS},
        })

    if method == "tools/call":
        params = body.get("params", {})
        tool_name = params.get("name", "")
        arguments = params.get("arguments", {})

        handler = TOOL_HANDLERS.get(tool_name)
        if not handler:
            return JSONResponse({
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32601, "message": f"Unknown tool: {tool_name}"},
            })

        try:
            result_text = handler(arguments)
            return JSONResponse({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": result_text}],
                    "isError": False,
                },
            })
        except Exception as e:
            return JSONResponse({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": f"Error: {str(e)}"}],
                    "isError": True,
                },
            })

    return JSONResponse({
        "jsonrpc": "2.0",
        "id": req_id,
        "error": {"code": -32601, "message": f"Unsupported method: {method}"},
    })


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "tools": [t["name"] for t in MCP_TOOLS],
        "referrals_processed": len(REFERRAL_LOG),
        "followups_scheduled": len(FOLLOWUP_LOG),
    }
