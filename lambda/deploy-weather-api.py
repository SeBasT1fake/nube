import json
import boto3
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

apigateway_client = boto3.client('apigateway')
lambda_client = boto3.client('lambda')
iam_client = boto3.client('iam')

def lambda_handler(event, context):
    """
    Lambda para desplegar API Gateway con endpoints de consulta
    """
    try:
        api_name = event.get('api_name', 'weather-analytics-api')
        stage_name = event.get('stage_name', 'prod')
        lambda_function = event.get('lambda_function', 'weather-api-query-results')
        endpoints = event.get('endpoints', [])
        cors_enabled = event.get('cors_enabled', True)
        
        # Buscar o crear API
        api_id = find_or_create_api(api_name)
        logger.info(f"✅ API ID: {api_id}")
        
        # Obtener ARN de la función Lambda
        lambda_arn = get_lambda_arn(lambda_function)
        logger.info(f"✅ Lambda ARN: {lambda_arn}")
        
        # Obtener resource root
        resources = apigateway_client.get_resources(restApiId=api_id)
        root_resource_id = None
        for resource in resources['items']:
            if resource['path'] == '/':
                root_resource_id = resource['id']
                break
        
        created_endpoints = []
        
        # Crear recursos y métodos para cada endpoint
        for endpoint_path in endpoints:
            logger.info(f"Creando endpoint: {endpoint_path}")
            
            # Dividir path en segmentos
            path_parts = [part for part in endpoint_path.split('/') if part]
            
            current_resource_id = root_resource_id
            current_path = ''
            
            # Crear recursos anidados si es necesario
            for part in path_parts:
                current_path += f'/{part}'
                
                # Buscar si el recurso ya existe
                resource_exists = False
                for resource in resources['items']:
                    if resource['path'] == current_path:
                        current_resource_id = resource['id']
                        resource_exists = True
                        break
                
                # Crear recurso si no existe
                if not resource_exists:
                    response = apigateway_client.create_resource(
                        restApiId=api_id,
                        parentId=current_resource_id,
                        pathPart=part
                    )
                    current_resource_id = response['id']
                    logger.info(f"✅ Recurso creado: {current_path}")
            
            # Crear método GET
            try:
                apigateway_client.put_method(
                    restApiId=api_id,
                    resourceId=current_resource_id,
                    httpMethod='GET',
                    authorizationType='NONE'
                )
                logger.info(f"✅ Método GET creado para {endpoint_path}")
            except apigateway_client.exceptions.ConflictException:
                logger.info(f"Método GET ya existe para {endpoint_path}")
            
            # Configurar integración con Lambda
            integration_uri = f"arn:aws:apigateway:{boto3.Session().region_name}:lambda:path/2015-03-31/functions/{lambda_arn}/invocations"
            
            try:
                apigateway_client.put_integration(
                    restApiId=api_id,
                    resourceId=current_resource_id,
                    httpMethod='GET',
                    type='AWS_PROXY',
                    integrationHttpMethod='POST',
                    uri=integration_uri
                )
                logger.info(f"✅ Integración Lambda configurada para {endpoint_path}")
            except apigateway_client.exceptions.ConflictException:
                logger.info(f"Integración ya existe para {endpoint_path}")
            
            # Configurar CORS si está habilitado
            if cors_enabled:
                setup_cors(api_id, current_resource_id)
            
            # Dar permisos a API Gateway para invocar Lambda
            add_lambda_permission(lambda_function, api_id, endpoint_path)
            
            created_endpoints.append({
                'path': endpoint_path,
                'method': 'GET',
                'resource_id': current_resource_id,
                'integration': 'AWS_PROXY'
            })
        
        # Crear deployment
        deployment_response = apigateway_client.create_deployment(
            restApiId=api_id,
            stageName=stage_name,
            description=f'Weather Analytics API deployment - {datetime.now().isoformat()}'
        )
        
        # URL base del API
        region = boto3.Session().region_name
        api_url = f"https://{api_id}.execute-api.{region}.amazonaws.com/{stage_name}"
        
        # Crear documentación básica
        endpoint_docs = []
        for endpoint in endpoints:
            endpoint_docs.append({
                'path': f"{api_url}{endpoint}",
                'method': 'GET',
                'description': get_endpoint_description(endpoint)
            })
        
        logger.info(f"✅ API desplegada: {api_url}")
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'API Gateway desplegado exitosamente',
                'api_id': api_id,
                'api_url': api_url,
                'stage': stage_name,
                'endpoints': endpoint_docs,
                'deployment_id': deployment_response['id'],
                'lambda_function': lambda_function,
                'cors_enabled': cors_enabled,
                'timestamp': datetime.now().isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"Error desplegando API Gateway: {str(e)}")
        return {
            'statusCode': 500,
            'body': {'error': str(e)}
        }

def find_or_create_api(api_name):
    """Buscar API existente o crear nueva"""
    # Buscar API existente
    apis = apigateway_client.get_rest_apis()
    for api in apis['items']:
        if api['name'] == api_name:
            return api['id']
    
    # Crear nueva API
    response = apigateway_client.create_rest_api(
        name=api_name,
        description='Weather Analytics API for querying processed climate data',
        endpointConfiguration={'types': ['REGIONAL']}
    )
    return response['id']

def get_lambda_arn(function_name):
    """Obtener ARN de función Lambda"""
    try:
        response = lambda_client.get_function(FunctionName=function_name)
        return response['Configuration']['FunctionArn']
    except lambda_client.exceptions.ResourceNotFoundException:
        raise Exception(f"Lambda function {function_name} not found")

def setup_cors(api_id, resource_id):
    """Configurar CORS para un recurso"""
    try:
        # Crear método OPTIONS
        apigateway_client.put_method(
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod='OPTIONS',
            authorizationType='NONE'
        )
        
        # Configurar integración MOCK para OPTIONS
        apigateway_client.put_integration(
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod='OPTIONS',
            type='MOCK',
            requestTemplates={'application/json': '{"statusCode": 200}'}
        )
        
        # Configurar respuesta para OPTIONS
        apigateway_client.put_method_response(
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod='OPTIONS',
            statusCode='200',
            responseParameters={
                'method.response.header.Access-Control-Allow-Headers': False,
                'method.response.header.Access-Control-Allow-Methods': False,
                'method.response.header.Access-Control-Allow-Origin': False
            }
        )
        
        # Configurar integración response para OPTIONS
        apigateway_client.put_integration_response(
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod='OPTIONS',
            statusCode='200',
            responseParameters={
                'method.response.header.Access-Control-Allow-Headers': "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'",
                'method.response.header.Access-Control-Allow-Methods': "'GET,OPTIONS'",
                'method.response.header.Access-Control-Allow-Origin': "'*'"
            }
        )
        
    except apigateway_client.exceptions.ConflictException:
        pass  # CORS ya configurado

def add_lambda_permission(function_name, api_id, endpoint_path):
    """Dar permisos a API Gateway para invocar Lambda"""
    try:
        statement_id = f"api-gateway-{api_id}-{endpoint_path.replace('/', '-')}"
        region = boto3.Session().region_name
        account_id = boto3.client('sts').get_caller_identity()['Account']
        
        source_arn = f"arn:aws:execute-api:{region}:{account_id}:{api_id}/*/*/*"
        
        lambda_client.add_permission(
            FunctionName=function_name,
            StatementId=statement_id,
            Action='lambda:InvokeFunction',
            Principal='apigateway.amazonaws.com',
            SourceArn=source_arn
        )
        
    except lambda_client.exceptions.ResourceConflictException:
        pass  # Permiso ya existe

def get_endpoint_description(endpoint):
    """Obtener descripción para cada endpoint"""
    descriptions = {
        '/weather/cities': 'Obtener estadísticas por ciudad',
        '/weather/monthly': 'Obtener patrones mensuales',
        '/weather/extreme-events': 'Obtener eventos climáticos extremos',
        '/weather/precipitation': 'Obtener datos de precipitación',
        '/weather/ml-predictions': 'Obtener predicciones de ML',
        '/weather/summary': 'Obtener resumen general',
        '/weather/health': 'Health check del API'
    }
    return descriptions.get(endpoint, 'Endpoint de consulta de datos climáticos')

# Función para testing
def test_api_deployment():
    """Probar despliegue de API"""
    test_event = {
        'api_name': 'weather-analytics-api',
        'stage_name': 'prod',
        'lambda_function': 'weather-api-query-results',
        'endpoints': [
            '/weather/cities',
            '/weather/monthly',
            '/weather/health'
        ],
        'cors_enabled': True
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    test_api_deployment()