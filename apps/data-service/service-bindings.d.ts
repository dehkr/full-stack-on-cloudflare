interface DestinationStatusEvaluationParams {
	linkId: string;
	destinationUrl: string;
	accountId: string;
}

interface ClickBatchEtlParams {
	dayDate: string;
}

interface Env extends Cloudflare.Env {
	CLICK_ETL_WORKFLOW: Workflow<ClickBatchEtlParams>;
	DESTINATION_STATUS_EVALUATION: Workflow<DestinationStatusEvaluationParams>;
}
