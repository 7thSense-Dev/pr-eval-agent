"""
Provider Factory for Conversation Pipeline
File: providers/provider_factory.py
"""

from typing import Optional
import structlog

from providers.base_provider import BaseConversationProvider
from providers.claude_provider import ClaudeProvider
from providers.openai_provider import OpenAIProvider

logger = structlog.get_logger()


class ProviderFactory:
    """
    Factory for creating conversation providers
    Similar to ProviderManager in your LLM routing service
    """
    
    # Registry of available providers
    _providers = {
        'claude': ClaudeProvider,
        'openai': OpenAIProvider
    }
    
    @classmethod
    def create_provider(cls, provider_id: str) -> BaseConversationProvider:
        """
        Create a provider instance
        
        Args:
            provider_id: Provider identifier ('claude' or 'openai')
            
        Returns:
            BaseConversationProvider instance
        """
        if provider_id not in cls._providers:
            raise ValueError(
                f"Unknown provider: {provider_id}. "
                f"Available: {list(cls._providers.keys())}"
            )
        
        provider_class = cls._providers[provider_id]
        logger.info(
            "Creating provider",
            provider_id=provider_id,
            provider_class=provider_class.__name__
        )
        
        return provider_class()
    
    @classmethod
    def register_provider(cls, provider_id: str, provider_class: type):
        """
        Register a new provider
        
        Args:
            provider_id: Provider identifier
            provider_class: Provider class (must inherit from BaseConversationProvider)
        """
        if not issubclass(provider_class, BaseConversationProvider):
            raise TypeError(
                f"{provider_class.__name__} must inherit from BaseConversationProvider"
            )
        
        cls._providers[provider_id] = provider_class
        logger.info(
            "Provider registered",
            provider_id=provider_id,
            provider_class=provider_class.__name__
        )
    
    @classmethod
    def list_providers(cls) -> list:
        """List all registered provider IDs"""
        return list(cls._providers.keys())
    
    @classmethod
    def get_provider(cls, provider_id: str) -> Optional[type]:
        """Get provider class by ID"""
        return cls._providers.get(provider_id)


# Convenience function
def create_provider(provider_id: str = 'claude') -> BaseConversationProvider:
    """
    Convenience function to create a provider
    
    Args:
        provider_id: Provider identifier (default: 'claude')
        
    Returns:
        BaseConversationProvider instance
    """
    return ProviderFactory.create_provider(provider_id)